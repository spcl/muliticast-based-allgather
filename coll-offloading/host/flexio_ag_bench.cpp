#include <iostream>
#include <cassert>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <iomanip>
#include <new>
#include <signal.h>
#include <sched.h>
#include <pthread.h>

#include "utils.hpp"
#include "cxxopts.hpp"
#include "transport.hpp"

#include "flexio_ag_bench_com.hpp"

#define BATCHED_IMM_WRITE

struct bmark_ctx
{
    bmark_ctx(const std::string &bmark_name, const cxxopts::ParseResult &opts)
        :
        transport(opts["transport"].as<std::string>()),
        name(bmark_name),
        n_iters(opts["iters"].as<std::size_t>()),
        warmup(opts["warmup"].as<std::size_t>()),
        payload_size(opts["payload_size"].as<std::size_t>()),
        tput_data(opts),
        lat_data(opts)
    {}

    bmark_ctx(const struct bmark_ctx &obj)
        :
        transport(obj.transport),
        name(obj.name),
        n_iters(obj.n_iters),
        warmup(obj.warmup),
        payload_size(obj.payload_size),
        tput_data(obj.tput_data),
        lat_data(obj.lat_data)
    {}

    static void header_print()
    {
        std::cout << "transport;bmark;msg_size;n_threads;chunk_size;txq_len;tx_window;tput(gbit/s);rate(mmps);rate(mcps);lat(us)" << std::endl;
    }

    void stats_print() const
    {
        std::cout << transport;
        std::cout << ";" << name;
        std::cout << ";" << payload_size;
        std::cout << ";" << tput_data.n_threads;
        std::cout << ";" << tput_data.chunk_size;
        std::cout << ";" << tput_data.txq_len;
        std::cout << ";" << tput_data.tx_window;
        std::cout << ";" << tput_data.tput_avg;
        std::cout << ";" << tput_data.msg_rate_avg;
        std::cout << ";" << tput_data.chunk_inj_rate_avg;
        std::cout << ";" << std::setprecision(8) << lat_data.t_avg << std::endl;
    }

    /* Global params */
    const std::string transport;
    const std::string name;
    const std::size_t n_iters;
    const std::size_t warmup;
    const std::size_t payload_size;

    /* Benchmark specific params */
    struct tput {
        tput(const cxxopts::ParseResult &opts)
            :
            tx_window(opts["tx_window"].as<std::size_t>()),
            txq_len(opts["epq_len"].as<std::size_t>()),
            chunk_size(opts["chunk_size"].as<std::size_t>()),
            n_threads(opts["epn"].as<std::size_t>())
        {
            assert(tx_window <= opts["epq_len"].as<std::size_t>());
        }

        tput(const struct tput &obj) // copy constructor
            :
            tx_window(obj.tx_window),
            txq_len(obj.txq_len),
            chunk_size(obj.chunk_size),
            n_threads(obj.n_threads)
        {
            t_start            = obj.t_start;
            t_end              = obj.t_end;
            tput_avg           = obj.tput_avg;
            chunk_inj_rate_avg = obj.chunk_inj_rate_avg;
            msg_rate_avg       = obj.msg_rate_avg;
        }

        const std::size_t tx_window;
        const std::size_t txq_len;
        const std::size_t chunk_size;
        const std::size_t n_threads;

        /* Results */
        std::vector<double> t_start;
        std::vector<double> t_end;
        double tput_avg           = 0.0;
        double chunk_inj_rate_avg = 0.0;
        double msg_rate_avg       = 0.0;
    } tput_data;

    struct lat {
        lat(const cxxopts::ParseResult &opts)
        {
            (void)opts; // TBD
        }

        lat(const struct lat &obj)
        {
            t_start = obj.t_start;
            t_end   = obj.t_end;
            t_avg   = obj.t_avg;
        }

        std::vector<double> t_start;
        std::vector<double> t_end;
        double t_avg = 0.0;
    } lat_data;
};

const int bmark_cm_default_port = 2000;
const int bmark_cm_daemon_default_port = 2001;

template <class cdev, class cpe, class cep,
          class sdev, class spe, class sep>
class bmark
{

public:

    // FIXME: Deprecated APIs:
    void dpa_pingpong_server(struct dev_ctx *dev, struct ep_ctx *ep,
                             struct ibv_mr *mkey, void *buf_ptr,
                             std::size_t dgram_size, std::size_t base_info_size)
    {
        struct dpa_pingpong_server_ctx ctx;
        ctx.cq_id          = ep->_idx;
        ctx.qp_id          = ep->_pe->_idx;
        ctx.dgram_size     = dgram_size;
        ctx.base_info_size = base_info_size;
        ctx.lkey           = mkey->lkey;
        ctx.ptr            = reinterpret_cast<uint64_t>(buf_ptr);

        auto dpa_dev = static_cast<dpa_dev_ctx *>(dev);
        dpa_dev->app_ctx_export(DPA_EH_PINGPONG_SERVER, &ctx, sizeof(ctx));

        ep->post_recv(mkey, buf_ptr, dgram_size);

        _cm_bmark->barrier(); // RNR barrier.

        /*
         * All server-side handshake gets processed on DPA.
         * Event handler initiated on Ping receive CQE sends Pong from the host memory back to the client and also polls the corresponding completion.
         */

        _cm_bmark->barrier();
    }

    // FIXME: Deprecated APIs.
    struct bmark_ctx lat()
    {
        struct bmark_ctx ctx("lat", _opts);

        auto n_iters        = ctx.n_iters;
        auto payload_size   = ctx.payload_size;
        auto max_dgram_size = payload_size;
        auto buf_len        = 2 * max_dgram_size;
        auto buf_ptr        = new (std::align_val_t(64)) char[buf_len];
        auto buf_mkey       = _dev->reg_mr(buf_ptr, buf_len);
        auto is_client      = _cm_bmark->get_id();
        auto &ep            = _eps[0]; // we will use the 0th EP/PE pair for this test 

        if (is_client) {
            auto &t_start = ctx.lat_data.t_start;
            auto &t_end   = ctx.lat_data.t_end;
            auto &t_avg   = ctx.lat_data.t_avg;

            t_start.resize(n_iters);
            t_end.resize(n_iters);

            for (std::size_t iter = 0; iter < n_iters; iter++) {
                _cm_bmark->barrier(); // prevent receiver not ready (RNR)
                t_start[iter] = _cm_bmark->time();
                ep->post_recv(buf_mkey, buf_ptr + max_dgram_size, max_dgram_size);
                ep->post_send(buf_mkey, buf_ptr, payload_size);
                auto comps = ep->_pe->poll(); // poll PE that was used to create this EP
                if (comps == 1)
                    ep->_pe->poll();
                t_end[iter] = _cm_bmark->time();
                _cm_bmark->barrier();
            }

            for (std::size_t iter = 0; iter < n_iters; iter++)
                t_avg += (t_end[iter] - t_start[iter]) / 2.0;
            t_avg /= n_iters;
            t_avg *= 1e6;
        } else {
            for (std::size_t iter = 0; iter < n_iters; iter++) {
                if (typeid(sep) == typeid(class dpa_rc_ep_ctx)) {
                    dpa_pingpong_server(_dev, ep, buf_mkey, buf_ptr, max_dgram_size, payload_size);
                    continue;
                }

                ep->post_recv(buf_mkey, buf_ptr, max_dgram_size);
                _cm_bmark->barrier();
                ep->_pe->poll();
                ep->post_send(buf_mkey, buf_ptr + max_dgram_size, payload_size);
                ep->_pe->poll();
                _cm_bmark->barrier();
            }
        }

        _dev->dereg_mr(buf_mkey);
        delete[] buf_ptr;

        return ctx;
    }

    void dpa_tput_server_prepost(std::size_t chunk_size)
    {
        for (auto &ep : _eps) {
            for (std::size_t i = 0; i < ep->_q_len; i++) {
                ep->post_recv(nullptr, nullptr, chunk_size, i);
            }
        }
    }

    void
    dpa_tput_server_run(std::size_t to_process, std::size_t chunk_size,
                        struct ibv_mr *payload_mkey, void *payload_buf,
                        struct ibv_mr *ack_mkey, uint64_t *ack_buf,
                        struct ibv_mr *receiver_ready_mr, uint64_t *receiver_ready_signal,
                        uint32_t iter)
    {
        const std::size_t n_counters = MAX_FLEXIO_CQS; // seems like reg_mr needs >=64 Bytes
        volatile auto counter        = new (std::align_val_t(64)) uint64_t[n_counters];
        auto counter_mkey            = _dev->reg_mr(counter, sizeof(*counter) * n_counters);
        struct dpa_tput_ctx ctx;

        ctx.worker_id     = 0;
        ctx.finisher_id   = 0;
        ctx.n_workers     = _eps.size();
        ctx.chunk_size    = chunk_size;
        ctx.to_process    = to_process / ctx.n_workers;
        ctx.iter          = iter;
        ctx.payload_rkey  = payload_mkey->rkey;
        ctx.payload_ptr   = reinterpret_cast<uint64_t>(payload_buf);
        ctx.ack_lkey      = ack_mkey->lkey;
        ctx.ack_ptr       = reinterpret_cast<uint64_t>(ack_buf);
        ctx.counter_lkey  = counter_mkey->lkey;
        ctx.counter_ptr   = reinterpret_cast<uint64_t>(counter);

        assert(to_process % ctx.n_workers == 0);

        auto dpa_dev = static_cast<dpa_dev_ctx *>(_dev);
        dpa_dev->app_ctx_export(typeid(sep) == typeid(struct dpa_rc_staging_ep_ctx) ?
                                DPA_EH_TPUT_STAGING_SERVER :
                                DPA_EH_TPUT_SERVER,
                                &ctx, sizeof(ctx));
        uint64_t val = 0;
        *counter = 0; // Updated by the last one active thread on DPA

        _aux_ep->post_send(receiver_ready_mr, receiver_ready_signal, sizeof(*receiver_ready_signal));
        auto n_comps = _aux_ep->_pe->poll();
        assert(n_comps == 1);

        while(val != (DPA_WORK_COMPLETED_MAGICNUM + iter))
            val = counter[0];

        _dev->dereg_mr(counter_mkey);
        delete[] counter;
    }

    void
    dpa_tput_client_run(std::size_t to_process, std::size_t chunk_size, std::size_t tx_window,
                        void *payload_buf, struct ibv_mr *payload_mkey,
                        struct packed_mkey &packed_rkey,
                        struct ibv_mr *ack_mkey, uint64_t *ack,
                        struct ibv_mr *receiver_ready_mr, uint64_t *receiver_ready_signal,
                        double &t_start, double &t_end,
                        uint32_t iter)
    {
        const std::size_t n_counters = MAX_FLEXIO_CQS; // seems like reg_mr needs >=64 Bytes
        volatile auto counter        = new (std::align_val_t(64)) uint64_t[n_counters];
        auto counter_mkey            = _dev->reg_mr(counter, sizeof(*counter) * n_counters);
        struct dpa_tput_ctx ctx;

        ctx.worker_id     = 0;
        ctx.finisher_id   = 0;
        ctx.n_workers     = _eps.size();
        ctx.chunk_size    = chunk_size;
        ctx.tx_window     = tx_window;
        ctx.iter          = iter;
        ctx.to_process    = to_process / ctx.n_workers;
        ctx.payload_rkey  = payload_mkey->rkey;
        ctx.payload_ptr   = reinterpret_cast<uint64_t>(payload_buf);
        ctx.ack_lkey      = packed_rkey.rkey; // we have nothing to do with ack in EH explicitly, so we are safe re-use ack-related fields
        ctx.ack_ptr       = packed_rkey.rptr;
        ctx.counter_lkey  = counter_mkey->lkey;
        ctx.counter_ptr   = reinterpret_cast<uint64_t>(counter);

        assert(to_process % ctx.n_workers == 0);

        auto dpa_dev = static_cast<dpa_dev_ctx *>(_dev);
        dpa_dev->app_ctx_export(DPA_EH_TPUT_CLIENT, &ctx, sizeof(ctx));

        memset(counter, 0, sizeof(*counter) * n_counters);

        _aux_ep->post_recv(receiver_ready_mr, receiver_ready_signal, sizeof(*receiver_ready_signal));
        auto n_comps = _aux_ep->_pe->poll();
        assert(n_comps == 1);

        t_start = _cm_bmark->time();

        for (auto &ep : _eps) // Will that work with UD/STAGING?
            ep->post_recv(ack_mkey, ack, sizeof(*ack), 0);

        // Note1: host2dpa transfers below will only *activate* DPA threads
        // Note2: we split it into the two loops to make sure that all EPs start as close to each other as possible
        // Note3: this will break in case of non-symmetric PE:EP subscription ratios

        for (auto &ep : _eps)
            static_cast<struct dpa_rc_ep_ctx *>(ep)->post_recv_host2dpa(nullptr, nullptr, 0, 0);
        for (auto &ep : _eps)
            static_cast<struct dpa_rc_ep_ctx *>(ep)->post_send_host2dpa(nullptr, nullptr, 0, 0);

        uint64_t val = 0;
        while(val != (DPA_WORK_COMPLETED_MAGICNUM + iter))
            val = counter[0];

        t_end = _cm_bmark->time();

        _dev->dereg_mr(counter_mkey);
        delete[] counter;
    }

    void
    host_tput_server_run(std::size_t to_process, struct ibv_mr *ack_mkey, uint64_t *ack,
                         struct ibv_mr *receiver_ready_mr, uint64_t *receiver_ready_signal)
    {
        auto run_infinitely = _opts["pseudo_run_infinitely"].count();
        auto to_post = to_process;

        assert((_eps.size() == 1) && (_pes.size() == 1)); // host supports only 1 PE
        for (std::size_t w = 0; (w < _eps[0]->_q_len) and (to_post > 0); w++, to_post--)
            _eps[0]->post_recv(nullptr, nullptr, 0, w);

        _aux_ep->post_send(receiver_ready_mr, receiver_ready_signal, sizeof(*receiver_ready_signal));
        auto n_comps = _aux_ep->_pe->poll();
        assert(n_comps == 1);

        while (to_process > 0) {
            std::size_t comps = _eps[0]->_pe->poll();
            for (std::size_t w = 0; (w < comps) and (to_post > 0); w++, to_post--)
                _eps[0]->post_recv(nullptr, nullptr, 0, w);
            to_process -= comps;
            if (run_infinitely && !(to_process % 12345678)) {
                std::cerr << "remaining chunks: " << to_process << std::endl;
            }
        }

        _eps[0]->post_send(ack_mkey, ack, sizeof(*ack), 0);
        _eps[0]->_pe->poll();
    }

    static const uint32_t bmark_client_workers_start_signal = 0xDEADBEAF;
    static const uint32_t bmark_client_workers_finished_signal = 0xDEADCAFE;

    static void
    host_tput_client_worker(std::size_t my_pe_id,
                            char *payload_ptr,
                            struct ibv_mr *payload_mkey,
                            struct packed_mkey &packed_rkey,
                            std::size_t payload_size,
                            std::size_t chunk_size,
                            std::size_t tx_window,
                            struct ibv_mr *ack_mkey,
                            uint64_t *ack,
                            const uint64_t ack_wr_id,
                            std::size_t ack_counter,
                            const std::size_t to_post_per_ep,
                            std::size_t to_process_per_worker,
                            std::vector<std::size_t> &to_post,
                            std::vector<std::size_t> &poffset,
                            std::vector<std::size_t> &batch_size,
                            std::vector<uint32_t> &chunk_offset,
                            uint32_t subscr_ratio,
                            std::vector<struct pe_ctx *> &pes,
                            std::vector<struct ep_ctx *> &eps,
                            std::atomic<uint32_t> &activation_signal,
                            std::atomic<uint32_t> &n_active)
    {
        while(activation_signal.load(std::memory_order_seq_cst) != bmark_client_workers_start_signal)
            ;
        n_active.fetch_add(1, std::memory_order_seq_cst);

        if (activation_signal.load(std::memory_order_seq_cst) == bmark_client_workers_start_signal) {
            auto pe                 = pes[my_pe_id];
            std::size_t ep_start_id = my_pe_id * subscr_ratio;
            std::size_t ep_end_id   = ep_start_id + subscr_ratio;

            assert(ep_end_id <= eps.size());

            for (std::size_t ep_id = ep_start_id; ep_id < ep_end_id; ep_id++) {
                eps[ep_id]->post_recv(ack_mkey, ack, sizeof(*ack), ack_wr_id);
                poffset[ep_id] = ep_id * to_post_per_ep * chunk_size;
            }

            /* Pre-post loop */
#ifndef BATCHED_IMM_WRITE
            for (std::size_t w = 0; w < tx_window; w++) { // round robin
#else
#endif
                for (std::size_t ep_id = ep_start_id; ep_id < ep_end_id; ep_id++) {
                    if (to_post[ep_id] > 0) {
#ifndef BATCHED_IMM_WRITE
                        if (typeid(sep) != typeid(struct dpa_rc_staging_ep_ctx)) {
                            eps[ep_id]->post_write_with_imm(payload_mkey,
                                                            payload_ptr + poffset[ep_id],
                                                            packed_rkey.rkey,
                                                            packed_rkey.rptr + poffset[ep_id],
                                                            chunk_size,
                                                            ep_id);
                        } else {
                            eps[ep_id]->post_send(payload_mkey,
                                                  payload_ptr + poffset[ep_id],
                                                  chunk_size,
                                                  ep_id);
                        }
                        to_post[ep_id]--;
                        poffset[ep_id] += chunk_size;
                        poffset[ep_id] %= payload_size; // wrap up in case of infinite run
#else
                        batch_size[ep_id] = to_post_per_ep < tx_window ? to_post_per_ep : tx_window;
                        if (typeid(sep) != typeid(struct dpa_rc_staging_ep_ctx)) {
                            eps[ep_id]->post_write_with_imm_batch(payload_mkey,
                                                                  payload_ptr + poffset[ep_id],
                                                                  packed_rkey.rkey,
                                                                  packed_rkey.rptr + poffset[ep_id],
                                                                  chunk_size,
                                                                  batch_size[ep_id],
                                                                  ep_id,
                                                                  chunk_offset[ep_id]);
                        } else {
                            eps[ep_id]->post_send_batch(payload_mkey,
                                                        payload_ptr + poffset[ep_id],
                                                        chunk_size,
                                                        batch_size[ep_id],
                                                        ep_id,
                                                        chunk_offset[ep_id]);
                        }
                        to_post[ep_id]      -= batch_size[ep_id];
                        chunk_offset[ep_id] += batch_size[ep_id];
                        poffset[ep_id]      += chunk_offset[ep_id] * chunk_size;
                        poffset[ep_id]      %= payload_size;
#endif
                    }
                }
#ifndef BATCHED_IMM_WRITE
            }
#endif

            /* Re-post loop */
            struct ibv_wc *wcs_list; // TODO: how to hide IBV completion semantics?          
            while (to_process_per_worker > 0) {
                auto comps = pe->poll(&wcs_list);
                
                for (auto i = 0; i < comps; i++) {
                    auto ep_id = wcs_list[i].wr_id;
                    if (ep_id != ack_wr_id) {
#ifndef BATCHED_IMM_WRITE
                        assert(to_process_per_worker >= to_post[ep_id]);
                        to_process_per_worker--;

                        if (to_post[ep_id] > 0) {
                            if (typeid(sep) != typeid(struct dpa_rc_staging_ep_ctx)) {
                                eps[ep_id]->post_write_with_imm(payload_mkey,
                                                                payload_ptr + poffset[ep_id],
                                                                packed_rkey.rkey,
                                                                packed_rkey.rptr + poffset[ep_id],
                                                                chunk_size,
                                                                ep_id);
                            } else {
                                eps[ep_id]->post_send(payload_mkey,
                                                      payload_ptr + poffset[ep_id],
                                                      chunk_size,
                                                      ep_id);
                            }
                            to_post[ep_id]--;
                            poffset[ep_id] += chunk_size;
                            poffset[ep_id] %= payload_size;
                        }
#else
                        assert(to_process_per_worker >= to_post[ep_id]);
                        to_process_per_worker -= batch_size[ep_id];

                        if (to_post[ep_id] > 0) {
                            batch_size[ep_id] = to_post[ep_id] < tx_window ? to_post[ep_id] : tx_window;
                            if (typeid(sep) != typeid(struct dpa_rc_staging_ep_ctx)) {
                                eps[ep_id]->post_write_with_imm_batch(payload_mkey,
                                                                      payload_ptr + poffset[ep_id],
                                                                      packed_rkey.rkey,
                                                                      packed_rkey.rptr + poffset[ep_id],
                                                                      chunk_size,
                                                                      batch_size[ep_id],
                                                                      ep_id,
                                                                      chunk_offset[ep_id]);
                            } else {
                                eps[ep_id]->post_send_batch(payload_mkey,
                                                            payload_ptr + poffset[ep_id],
                                                            chunk_size,
                                                            batch_size[ep_id],
                                                            ep_id,
                                                            chunk_offset[ep_id]);
                            }
                            to_post[ep_id]      -= batch_size[ep_id];
                            poffset[ep_id]      += chunk_size * batch_size[ep_id];
                            poffset[ep_id]      %= payload_size;
                            chunk_offset[ep_id] += batch_size[ep_id];
                        }
#endif
                    } else {
                        ack_counter--;
                    }
                }
            }

            /* Ack loop */ 
            if (ack_counter) {
                assert(ack_counter <= subscr_ratio);
                while (ack_counter) {
                    ack_counter -= pe->poll(&wcs_list);
                }
            }

            asm volatile("" ::: "memory");
            if (n_active.fetch_sub(1, std::memory_order_seq_cst) == 1) { // only the last one thread enters it
                assert(ack_counter == 0);
                assert(n_active.load(std::memory_order_seq_cst) == 0);
                activation_signal.store(bmark_client_workers_finished_signal, std::memory_order_seq_cst);
            }
        } else {
            std::cerr << "This is a bug!"<< std::endl;
            assert(0);
        }
    }

    void
    host_tput_client_run(std::size_t to_process, std::size_t chunk_size, std::size_t tx_window,
                         char *payload_ptr, struct ibv_mr *payload_mkey, struct packed_mkey &packed_rkey,
                         std::size_t payload_size,
                         struct ibv_mr *ack_mkey, uint64_t *ack,
                         struct ibv_mr *receiver_ready_mr, uint64_t *receiver_ready_signal,
                         double &t_start, double &t_end)
    {
        const uint64_t ack_wr_id       = 0xDEADBEAF;
        std::size_t to_post_per_ep     = to_process / _eps.size();
        std::size_t to_post_per_worker = to_process / _pes.size();
        std::size_t pe_subscr_ratio    = _eps.size() / _pes.size();
        std::size_t ack_counter        = pe_subscr_ratio;
        std::vector<std::size_t> to_post(_eps.size(), to_post_per_ep);
        std::vector<std::size_t> poffset(_eps.size(), 0);
        std::vector<std::size_t> batch_size(_eps.size(), 0);
        std::vector<uint32_t> chunk_offset(_eps.size(), 0);
        std::atomic<uint32_t> activation_signal = 0;
        std::atomic<uint32_t> n_active = 0;

        assert(to_process % _eps.size() == 0);

        _aux_ep->post_recv(receiver_ready_mr, receiver_ready_signal, sizeof(*receiver_ready_signal));
        auto n_comps = _aux_ep->_pe->poll();
        assert(n_comps == 1);

        std::vector<std::thread> workers;
        for (std::size_t pe_id = 0; pe_id < _pes.size(); pe_id++) {
            workers.push_back(std::thread(host_tput_client_worker, pe_id,
                                          payload_ptr, payload_mkey, std::ref(packed_rkey), payload_size, chunk_size, tx_window,
                                          ack_mkey, ack, ack_wr_id, ack_counter,
                                          to_post_per_ep, to_post_per_worker,
                                          std::ref(to_post), std::ref(poffset), std::ref(batch_size), std::ref(chunk_offset), pe_subscr_ratio,
                                          std::ref(_pes), std::ref(_eps), std::ref(activation_signal), std::ref(n_active)));
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(pe_id, &cpuset);
            auto rc = pthread_setaffinity_np(workers[pe_id].native_handle(),
                                             sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                std::cerr << "Error calling pthread_setaffinity_np: " << rc << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        t_start = _cm_bmark->time();
        asm volatile("" ::: "memory");

        activation_signal.store(bmark_client_workers_start_signal, std::memory_order_seq_cst);
        while (activation_signal.load(std::memory_order_seq_cst) != bmark_client_workers_finished_signal)
            ;

        asm volatile("" ::: "memory");
        t_end = _cm_bmark->time();
        assert(t_start < t_end);

        while (workers.size()) {
            workers.back().join();
            workers.pop_back();
        }
    }

    void dcheck_buf_fill(void *ptr, std::size_t len, std::size_t iter)
    {
        assert(len > sizeof(std::size_t));
        assert(len % sizeof(std::size_t) == 0);

        for (std::size_t i = 0; i < len / sizeof(std::size_t); i++) {
            reinterpret_cast<std::size_t *>(ptr)[i] = i * 42 + iter;
        }
    }

    void dcheck_buf_verify(void *ptr, std::size_t len, std::size_t iter)
    {
        assert(len > sizeof(std::size_t));
        assert(len % sizeof(std::size_t) == 0);

        for (std::size_t i = 0; i < len / sizeof(std::size_t); i++) {
            if (reinterpret_cast<std::size_t *>(ptr)[i] != (i * 42 + iter)) {
                std::cerr << "Data check failed at pos=" << i << std::endl;
            }
        }
        std::cout << "Data check OK in iteration=" << iter << std::endl;
    }

    struct bmark_ctx tput()
    {
        struct bmark_ctx ctx("tput", _opts);

        auto is_client    = _cm_bmark->get_id();
        //FIXME: this is a workaround for our lab where only one side has DPA installed
        //if ((typeid(cpe) == typeid(struct dpa_pe_ctx)) and
        //    (typeid(spe) != typeid(struct dpa_pe_ctx)))
        //    is_client = !is_client;

        auto n_iters      = ctx.warmup + ctx.n_iters;
        auto payload_size = ctx.payload_size;
        auto tx_window    = ctx.tput_data.tx_window;
        auto chunk_size   = ctx.tput_data.chunk_size;

        bool remote_hmem            = _opts["hmem_daemon_addr"].as<std::string>().compare("");
        char *payload_ptr           = nullptr;
        struct ibv_mr *payload_mkey = nullptr;
        struct packed_mkey packed_lkey, packed_rkey, hmem_packed_key;
        memset(&packed_lkey, 0 , sizeof(packed_lkey));
        memset(&packed_rkey, 0 , sizeof(packed_rkey));
        memset(&hmem_packed_key, 0 , sizeof(hmem_packed_key));

        if (!remote_hmem || is_client) { // client side always allocates memory on the host
            payload_ptr  = new (std::align_val_t(4096)) char[payload_size];
            assert(reinterpret_cast<uint64_t>(payload_ptr) % 4096 == 0);
            payload_mkey = _dev->reg_mr(payload_ptr, payload_size);
            packed_lkey.lkey = payload_mkey->lkey;
            packed_lkey.rkey = payload_mkey->rkey;
            packed_lkey.rptr = reinterpret_cast<uint64_t>(payload_ptr);
        } else if (!is_client) { // server with enabled remote receive userbuf mode requests this buffer from daemon
            assert(typeid(sep) == typeid(struct dpa_rc_staging_ep_ctx));
            std::cerr << "Bmark server: requesting from daemon "
                      << payload_size << " Bytes" << std::endl;
            _cm_daemon->send_blocking(&payload_size, sizeof(payload_size)); // ask hmem daemon how much memory to allocate
            _cm_daemon->recv_blocking(&hmem_packed_key, sizeof(hmem_packed_key)); // get pointer and rkey to this memory
            std::cerr << "Bmark server: got packed mkey from daemon "
                      << hmem_packed_key.debug_get_str() << std::endl;
            assert(hmem_packed_key.rptr != 0);
            payload_ptr        = reinterpret_cast<char *>(hmem_packed_key.rptr);
            payload_mkey       = new struct ibv_mr;
            memset(payload_mkey, 0, sizeof(*payload_mkey));
            payload_mkey->rkey = hmem_packed_key.rkey;
        }

        _cm_bmark->exchange(&packed_lkey, &packed_rkey, sizeof(struct packed_mkey)); // this exchange is needed only for UC

        uint64_t ack_magic_num = 42 * 42;
        auto ack = new (std::align_val_t(64)) uint64_t;
        auto ack_mkey = _dev->reg_mr(ack, sizeof(*ack));

        auto receiver_ready_signal = new (std::align_val_t(64)) uint64_t;
        auto receiver_ready_mr = _aux_dev->reg_mr(receiver_ready_signal, sizeof(*receiver_ready_signal));

        auto &t_start = ctx.tput_data.t_start;
        auto &t_end   = ctx.tput_data.t_end;
        t_start.resize(n_iters);
        t_end.resize(n_iters);

        if (!is_client and
            ((typeid(sep) == typeid(struct dpa_rc_ep_ctx)) ||
             (typeid(sep) == typeid(struct dpa_rc_staging_ep_ctx))))
            dpa_tput_server_prepost(chunk_size);

        auto dcheck = _opts["dcheck"].count();
        auto run_infinitely = _opts["pseudo_run_infinitely"].count();

        for (std::size_t iter = 0; iter < n_iters; iter++) {
            size_t to_process = run_infinitely ? std::numeric_limits<std::size_t>::max() : (payload_size / chunk_size);
            if (!is_client) {                
                *ack = ack_magic_num;
                if ((typeid(sep) == typeid(struct dpa_rc_ep_ctx)) or
                    (typeid(sep) == typeid(struct dpa_rc_staging_ep_ctx))) {
                    dpa_tput_server_run(to_process, chunk_size, payload_mkey, payload_ptr, ack_mkey, ack,
                                        receiver_ready_mr, receiver_ready_signal, iter);
                } else {
                    host_tput_server_run(to_process, ack_mkey, ack, receiver_ready_mr, receiver_ready_signal);
                }
                if (dcheck)
                    dcheck_buf_verify(payload_ptr, payload_size, iter);
            } else {
                if (dcheck)
                    dcheck_buf_fill(payload_ptr, payload_size, iter);
                if (typeid(cep) == typeid(struct dpa_rc_ep_ctx)) {
                    dpa_tput_client_run(to_process, chunk_size, tx_window,
                                        payload_ptr, payload_mkey, packed_rkey,
                                        ack_mkey, ack, receiver_ready_mr, receiver_ready_signal,
                                        t_start[iter], t_end[iter], iter);
                } else {
                    host_tput_client_run(to_process, chunk_size, tx_window,
                                         payload_ptr, payload_mkey, packed_rkey, payload_size,
                                         ack_mkey, ack, receiver_ready_mr, receiver_ready_signal,
                                         t_start[iter], t_end[iter]);
                }
                if (_opts["log_per_iteration_time"].count()) {
                    std::cerr << iter << ";" << (t_end[iter] - t_start[iter]) * 1e6 << std::endl;
                }
            }
            if (!(iter % 100)) {
                std::cerr << "iteration: " << iter << "/" << n_iters << std::endl;
            }
        }

        if (is_client) {
            auto &tput_avg            = ctx.tput_data.tput_avg;
            auto &chunk_inj_rate_avg  = ctx.tput_data.chunk_inj_rate_avg;
            auto &msg_rate_avg        = ctx.tput_data.msg_rate_avg;
            for (std::size_t iter = ctx.warmup; iter < n_iters; iter++) {
                auto t_total       = t_end[iter] - t_start[iter];
                tput_avg           += static_cast<double>(payload_size) / t_total;
                chunk_inj_rate_avg += static_cast<double>(payload_size / chunk_size) / t_total;
                msg_rate_avg       += 1.0 / t_total;
            }
            tput_avg           /= ctx.n_iters;
            chunk_inj_rate_avg /= ctx.n_iters;
            msg_rate_avg       /= ctx.n_iters;
            tput_avg           /= 1e9; // GB/s
            chunk_inj_rate_avg /= 1e6; // Mcp/s
            msg_rate_avg       /= 1e6; // Mmp/s
        }

        _aux_dev->dereg_mr(receiver_ready_mr);
        delete receiver_ready_signal;
        _dev->dereg_mr(ack_mkey);
        delete ack;

        if (!(remote_hmem && !is_client)) {
            _dev->dereg_mr(payload_mkey);
            delete[] payload_ptr;
        } else {
            uint64_t dealloc_sig = 0xDEADBEAF;
            _cm_daemon->send_blocking(&dealloc_sig, sizeof(dealloc_sig));
            delete payload_mkey;
        }

        return ctx;
    }

    struct bmark_ctx run()
    {
        auto bmark_name = _opts["bmark"].as<std::string>(); 

        if (!bmark_name.compare("tput")) {
            return tput();
        } else if (!bmark_name.compare("lat")){
            return lat();
        }

        std::cerr << "Unknown benchmark " << bmark_name << " requested" << std::endl;
        exit(EXIT_FAILURE);

        return bmark_ctx("placeholder", _opts); // make compiler happy
    }

    void daemon_init()
    {
        _dev = new ib_dev_ctx(mlx5_iface_name);
        _pes.push_back(new ib_cq_pe_ctx(_dev, 1, 0, 1));
    }

    void daemon_run()
    {
        _cm_daemon->server_accept();

        std::size_t epn = 0;
        _cm_daemon->recv_blocking(&epn, sizeof(epn));
        std::cerr << "Daemon: server process asked to allocate " << epn << " EPs" << std::endl;
        struct ep_addr remote_ep_addr;
        for (std::size_t idx = 0; idx < epn; idx++) {
            _eps.push_back(new dummy_rc_ep_ctx(_dev, _pes[0]));
            std::cerr << "Daemon: send dummy EP addr " << _eps[idx]->_addr.debug_get_str() << std::endl;
            _cm_daemon->exchange(&_eps[idx]->_addr, &remote_ep_addr, sizeof(struct ep_addr));
            std::cerr << "Daemon: got new connection request from server EP "
                      << remote_ep_addr.debug_get_str() << std::endl;
            _eps[idx]->connect(remote_ep_addr);
        }

        std::size_t payload_size = 0;
        _cm_daemon->recv_blocking(&payload_size, sizeof(payload_size));
        assert(payload_size > 0);
        auto payload_ptr  = new (std::align_val_t(64)) char[payload_size];
        auto payload_mkey = _dev->reg_mr(payload_ptr, payload_size);
        struct packed_mkey key;
        key.lkey = payload_mkey->lkey;
        key.rkey = payload_mkey->rkey;
        key.rptr = reinterpret_cast<uint64_t>(payload_ptr);
        _cm_daemon->send_blocking(&key, sizeof(key));
        std::cerr << "Daemon: allocated " << payload_size
                  << " bytes mkey=" << key.debug_get_str()
                  << std::endl; 

        uint64_t dealloc_sig = 0;
        _cm_daemon->recv_blocking(&dealloc_sig, sizeof(uint64_t)); // blocks until all iterations will finish
        assert(dealloc_sig == 0xDEADBEAF);

        while (_eps.size()) {
            auto ep = _eps.back();
            delete ep;
            _eps.pop_back();
        }

        _dev->dereg_mr(payload_mkey);
        delete[] payload_ptr;

        std::cerr << "Daemon: buffer released" << std::endl; 

        _cm_daemon->data_sock_close();
    }

    bmark(struct oob_sock_ctx *cm,
          cxxopts::ParseResult &opts,
          bool run_daemon = false)
        :
        _opts(opts),
        _cm_bmark(cm),
        _cm_daemon(nullptr)
    {
        auto is_client       = _cm_bmark ? _cm_bmark->get_id() : 0;
        std::size_t pen      = _opts["pen"].as<std::size_t>();
        std::size_t epn      = _opts["epn"].as<std::size_t>();
        std::size_t peq_len  = _opts["peq_len"].as<std::size_t>();
        std::size_t epq_len  = _opts["epq_len"].as<std::size_t>();
        std::size_t tx_win   = _opts["tx_window"].as<std::size_t>();
        std::size_t iters    = _opts["iters"].as<std::size_t>();
        std::size_t warmup   = _opts["warmup"].as<std::size_t>();
        bool rnr_retry       = _opts["rnr_retry"].count();
        bool remote_hmem     = _opts["hmem_daemon_addr"].as<std::string>().compare("");
        int staging_mem_type = dpa_rc_staging_ep_ctx::str2memtype(_opts["staging_mem_type"].as<std::string>());

        //assert(pen == epn);
        assert(peq_len >= epq_len);
        assert(warmup < iters);

        if ((!is_client && _opts["hmem_daemon_addr"].as<std::string>().compare("")) || run_daemon) {
            _cm_daemon = new oob_sock_ctx(_opts["hmem_daemon_addr"].as<std::string>(),
                                          run_daemon ? true : false,
                                          run_daemon ? true : false,
                                          bmark_cm_daemon_default_port);
            if (!run_daemon) {
                _cm_daemon->send_blocking(&epn, sizeof(epn));
            }
        }

        if (run_daemon) {
            std::cerr << "Running host memory allocator daemon" << std::endl;
            daemon_init();
            return;
        } else {
            std::cerr << "Running benchmark datapath" << std::endl;
        }

        // FIXME: this is a workaround for our lab where only one side has DPA installed
        //if ((typeid(cpe) == typeid(struct dpa_pe_ctx)) and (typeid(spe) != typeid(struct dpa_pe_ctx)))
        //    is_client = !is_client;

        if (is_client)
            _dev = new cdev(mlx5_iface_name);
        else
            _dev = new sdev(mlx5_iface_name);

        for (std::size_t idx = 0; idx < pen; idx++) {
            if (is_client) {
                if (typeid(cpe) == typeid(struct dpa_pe_ctx)) {
                    // make network-side CQ passive
                    _pes.push_back(new dpa_pe_ctx(_dev, tx_win, idx, peq_len, false));
                } else {
                    _pes.push_back(new cpe(_dev, tx_win, idx, peq_len));
                }
            } else {
                _pes.push_back(new spe(_dev, tx_win, idx, peq_len));
            }
        }

        for (std::size_t idx = 0; idx < epn; idx++) {
            if (is_client) {
                if (typeid(cep) == typeid(struct dpa_rc_ep_ctx)) {
                    _eps.push_back(new cep(_dev, _pes[idx], idx, epq_len, rnr_retry));
                } else {
                    _eps.push_back(new cep(_dev, _pes[idx % _pes.size()], idx, epq_len, rnr_retry));
                }
                _eps.back()->_addr._is_client = true;
                std::cerr << "Bmark client: created new EP " << _eps.back()->_addr.debug_get_str() << std::endl;
            } else {
                if (typeid(sep) == typeid(struct dpa_rc_staging_ep_ctx)) {
                    _eps.push_back(new dpa_rc_staging_ep_ctx(_dev, _pes[idx], idx, epq_len,
                                                             rnr_retry,
                                                             staging_mem_type,
                                                             remote_hmem));
                    std::cerr << "Bmark server: created new EP " << _eps.back()->_addr.debug_get_str() << std::endl;
                    if (remote_hmem) {
                        struct ep_addr hmem_ep_addr;
                        _cm_daemon->exchange(&_eps.back()->_loopback_ep_addr, &hmem_ep_addr, sizeof(struct ep_addr));
                        std::cerr << "Bmark server: got new connection request from daemon EP " << hmem_ep_addr.debug_get_str() << std::endl;
                        static_cast<struct dpa_rc_staging_ep_ctx *>(_eps.back())->connect_to_hmem_daemon(hmem_ep_addr);
                        std::cerr << "Bmark server: loopback EP " << _eps.back()->_loopback_ep_addr.debug_get_str() << std::endl;
                    }
                } else {
                    _eps.push_back(new sep(_dev, _pes[idx], idx, epq_len, rnr_retry));
                }
            }
            _av.push_back(_eps.back()->_addr);
        }

        if ((is_client and (typeid(cdev) == typeid(class dpa_dev_ctx))) or
            (!is_client and (typeid(sdev) == typeid(class dpa_dev_ctx)))) {
            auto ctx_size = std::max(sizeof(struct dpa_pingpong_server_ctx),
                                     sizeof(struct dpa_tput_ctx));
            static_cast<struct dpa_dev_ctx *>(_dev)->app_ctx_alloc(ctx_size);
        }

        eps_connect();

        _aux_dev = new ib_dev_ctx(mlx5_iface_name);
        _aux_pe  = new ib_cq_pe_ctx(_aux_dev, tx_win, 0, peq_len);
        _aux_ep  = new rc_ep_ctx(_aux_dev, _aux_pe, 0, epq_len, true);
        struct ep_addr remote_aux_ep_addr;
        _cm_bmark->exchange(&_aux_ep->_addr, &remote_aux_ep_addr, sizeof(remote_aux_ep_addr));
        _aux_ep->connect(remote_aux_ep_addr);

        if (_opts["sleep_infinitely"].count()) {
            using namespace std::chrono_literals;
            while(true) {
                std::this_thread::sleep_for(1000ms);
            }
        }
    }

    ~bmark()
    {
        auto is_client = _cm_bmark->get_id();
        // FIXME: this is a workaround for our lab where only one side has DPA installed
        //if ((typeid(cpe) == typeid(struct dpa_pe_ctx)) and
        //    (typeid(spe) != typeid(struct dpa_pe_ctx)))
        //    is_client = !is_client;

        if ((is_client and (typeid(cdev) == typeid(class dpa_dev_ctx))) or
            (!is_client and (typeid(sdev) == typeid(class dpa_dev_ctx)))) {
            static_cast<struct dpa_dev_ctx *>(_dev)->app_ctx_free();
        }

        if (_cm_daemon) {
            delete _cm_daemon;
        } else {
            delete _aux_ep;
            delete _aux_pe;
            delete _aux_dev;
        }

        for (auto &_ep : _eps)
            delete _ep;
        for (auto &_pe : _pes)
            delete _pe;
        delete _dev;
    }

private:

    void eps_connect()
    {
        std::vector<struct ep_addr> remote_av(_av.size());
        _cm_bmark->exchange(_av.data(), remote_av.data(), sizeof(struct ep_addr) * _av.size());
        for (std::size_t i = 0; i < _av.size(); i++) {
            remote_av[i]._is_client ? assert(!_av[i]._is_client) : assert(_av[i]._is_client);
            _eps[i]->connect(remote_av[i]);
            std::cerr << "Bmark: connected EP=" << _eps[i]->_addr.debug_get_str() << " to EP=" << remote_av[i].debug_get_str() << std::endl;
        }
    }

    // Deprecated: Test each client/server EP pairs with uint64_t ping-pong
    bool eps_test_connectivity() 
    {
        auto dgram_size = sizeof(uint64_t);
        auto buf_len = 2 * dgram_size;
        auto buf_ptr = new char[buf_len];

        for (auto &ep : _eps) {
            auto mkey = _dev->reg_mr(buf_ptr, buf_len);
            if (ep->_addr._is_client) {
                ep->post_recv(mkey, buf_ptr + dgram_size, dgram_size);
                _cm_bmark->barrier(); // global barrier here to avoid server RNR
                ep->post_send(mkey, buf_ptr, sizeof(uint64_t)); // ping
                ep->_pe->poll(); // poll ping send
                ep->_pe->poll(); // poll pong recv
                _cm_bmark->barrier();
            } else { // server side
                if (typeid(sep) == typeid(class dpa_rc_ep_ctx)) {
                    dpa_pingpong_server(_dev, ep, mkey, buf_ptr, dgram_size, sizeof(uint64_t));
                } else {
                    ep->post_recv(mkey, buf_ptr, dgram_size);
                    _cm_bmark->barrier(); // global barrier here to avoid RNR
                    ep->_pe->poll(); // poll ping recv
                    ep->post_send(mkey, buf_ptr + dgram_size, sizeof(uint64_t));
                    ep->_pe->poll(); // poll pong send
                    _cm_bmark->barrier();
                }
            }
            _dev->dereg_mr(mkey);
        }

        delete[] buf_ptr;
        return true;
    }

    cxxopts::ParseResult &_opts;

    struct oob_sock_ctx *_cm_bmark;
    struct oob_sock_ctx *_cm_daemon;

    /* Benchmarked transport */
    struct dev_ctx *_dev;
    std::vector<struct pe_ctx *> _pes;
    std::vector<struct ep_ctx *> _eps;
    std::vector<struct ep_addr> _av;

    /* 
     * We use auxiliary RDMA path for barrier on the benchmark, since
     * sockets based barrier (i.e., oob_sock_ctx) blows up performance on DPU/ARM.
     */
    struct dev_ctx *_aux_dev;
    struct pe_ctx *_aux_pe;
    struct ep_ctx *_aux_ep;
};
    
void opts_parse(int argc, char **argv, cxxopts::ParseResult &opts)
{
    cxxopts::Options options("bmark", "Client/server-like performance benchmark for Allgather PoC");

    options.add_options()
        // Transport parameters
        ("rdma_dev", "RDMA device to use", cxxopts::value<std::string>()->default_value("mlx5_0"))
        ("t,transport", "EP/PE transport (host/dpa/dpa_dpu_proxy/dpa2dpa)", cxxopts::value<std::string>()->default_value("dpa"))
        ("b,bmark", "benchmark (lat/tput)", cxxopts::value<std::string>()->default_value("tput"))
        ("e,pen", "number of PEs", cxxopts::value<std::size_t>()->default_value("1"))
        ("p,epn", "number of EPs", cxxopts::value<std::size_t>()->default_value("1"))
        ("q,epq_len", "length of endpoint TX/RX request queue", cxxopts::value<std::size_t>()->default_value("8192"))
        ("s,peq_len", "length of the progress engine completion queue", cxxopts::value<std::size_t>()->default_value("8192"))
        ("hmem_daemon_addr", "IPv4 address of remote daemon to connect to", cxxopts::value<std::string>()->default_value(""))
        ("hmem_daemon", "run host memory allocator daemon")
        ("staging_mem_type", "specifiy staging memory type (host,dpa,memic)", cxxopts::value<std::string>()->default_value("host"))
        ("rnr_retry", "enable RC flow control to prevent RNR CQe error")
        // AG PoC parameters
        ("bmark_client_addr", "Benchmark client IPv4 address", cxxopts::value<std::string>()->default_value(""))
        ("m,payload_size", "payload size, Bytes", cxxopts::value<std::size_t>()->default_value("8388608"))
        ("w,tx_window", "TX window size, N", cxxopts::value<std::size_t>()->default_value("128"))
        ("c,chunk_size", "payload fragmentation chunk size, Bytes", cxxopts::value<std::size_t>()->default_value("4096"))
        ("n,iters", "Number of benchmark iterations, N", cxxopts::value<std::size_t>()->default_value("5000"))
        ("warmup", "Number of warmup iterations, N", cxxopts::value<std::size_t>()->default_value("200"))
        ("d,dcheck", "check payload correctness on the server side")
        ("v,print_header", "print benchmark header")
        ("log_per_iteration_time", "log per-iteration time in microseconds")
        ("sleep_infinitely", "sleep_infinitely after all EPs are connected")
        ("pseudo_run_infinitely", "each endpoint will send the number of chunks equeal to the std::size_t maximum value")
        ("h,help", "Print usage")

        ;

    opts = options.parse(argc, argv);

    if (opts.count("help")) {
        std::cout << options.help() << std::endl;
        exit(0);
    }
}

bool daemon_quit = false;

void daemon_signal_handler(int signum)
{
    if (signum == SIGINT || signum == SIGTERM) {
        std::cerr << "Daemon received signal " << signum;
        daemon_quit = true;
    }
}

int main(int argc, char **argv)
{
    cxxopts::ParseResult opts;
    opts_parse(argc, argv, opts);

    /*
     * This all is hard coded for our lab setup.
     * All the control path branchning below needs to generalized... 
     */

    mlx5_iface_name  = opts["rdma_dev"].as<std::string>(); // overwrite global variable defined in ibv_transport.hpp
    auto bmark_client_addr = opts["bmark_client_addr"].as<std::string>();
    bool is_server = bmark_client_addr.compare("") ? false : true;
    auto bmark_cm = opts.count("hmem_daemon") ? nullptr : new struct oob_sock_ctx(bmark_client_addr, is_server, false, bmark_cm_default_port);

    if (opts.count("hmem_daemon")) {
        struct bmark<struct ib_dev_ctx, struct ib_cq_pe_ctx, struct rc_ep_ctx,
                     struct ib_dev_ctx, struct ib_cq_pe_ctx, struct rc_ep_ctx> hmem_daemon(nullptr, opts, true);
        daemon_quit = false;
        signal(SIGINT, daemon_signal_handler);
        signal(SIGTERM, daemon_signal_handler);
        while(!daemon_quit)
            hmem_daemon.daemon_run();

        return 0;
    }

    //FIXME: Dirty hack to make DPU server accessible from remote machine
    is_server = !is_server;

    if (is_server && opts.count("print_header"))
        bmark_ctx::header_print();

    auto transport_name = opts["transport"].as<std::string>(); 
    /* still ugly */
    if (!transport_name.compare("dpa")) {
        struct bmark<struct ib_dev_ctx, struct ib_cq_pe_ctx, struct rc_ep_ctx,
                     struct dpa_dev_ctx, struct dpa_pe_ctx, struct dpa_rc_ep_ctx>
            dpa_rc_app(bmark_cm, opts);
        auto results = dpa_rc_app.run();
        if (!is_server) {
            results.stats_print();
        }
    } else if (!transport_name.compare("dpa_dpu_proxy")) {
        struct bmark<struct ib_dev_ctx, struct ib_cq_pe_ctx, struct rc_ep_ctx,
                     struct dpa_dev_ctx, struct dpa_pe_ctx, struct dpa_rc_staging_ep_ctx>
            dpa_rc_dpu_proxy_app(bmark_cm, opts);
        auto results = dpa_rc_dpu_proxy_app.run();
        if (!is_server) {
            results.stats_print();
        }
    } else if (!transport_name.compare("host")) {
        struct bmark<struct ib_dev_ctx, struct ib_cq_pe_ctx, struct rc_ep_ctx,
                     struct ib_dev_ctx, struct ib_cq_pe_ctx, struct rc_ep_ctx>
            rc_app(bmark_cm, opts);
        auto results = rc_app.run();
        if (!is_server) {
            results.stats_print();
        }
    } else if (!transport_name.compare("dpa2dpa")) {
        struct bmark<struct dpa_dev_ctx, struct dpa_pe_ctx, struct dpa_rc_ep_ctx,
                     struct dpa_dev_ctx, struct dpa_pe_ctx, struct dpa_rc_ep_ctx>
            dpa2dpa_app(bmark_cm, opts);
        auto results = dpa2dpa_app.run();
        if (!is_server) {
            results.stats_print();
        }
    } else if (!transport_name.compare("dpa2host")) {
        struct bmark<struct dpa_dev_ctx, struct dpa_pe_ctx, struct dpa_rc_ep_ctx,
                     struct ib_dev_ctx, struct ib_cq_pe_ctx, struct rc_ep_ctx>
            dpa2host_app(bmark_cm, opts);
        auto results = dpa2host_app.run();
        if (!is_server) {
            results.stats_print();
        }
        // FIXME: this is a workaround for our lab where only one side has DPA installed
        //if (!is_server) {
        //    results.stats_print();
        //}
    } else {
        std::cerr << "Unknown transport " << transport_name << " requested" << std::endl;
        exit(EXIT_FAILURE);
    }

    delete bmark_cm;

    return 0;
}
