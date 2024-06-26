import subprocess
import time
import sys
import copy
from pathlib import Path

class Transport:
    default_binary_name="release"
    default_binary_path                     ="/images/mkhalilov/flexio-builds/cc-host/apps/flexio_ag_bench/host/flexio_ag_bench"
    default_prof_binary_path                ="/images/mkhalilov/flexio-builds/cc-host-prof/apps/flexio_ag_bench/host/flexio_ag_bench"
    default_no_reliability_binary_path      ="/images/mkhalilov/flexio-builds/cc-host-no-rel/apps/flexio_ag_bench/host/flexio_ag_bench"
    default_bitmap_binary_path              ="/images/mkhalilov/flexio-builds/cc-host/apps/flexio_ag_bench/host/flexio_ag_bench"
    default_bitmap_opt_binary_path          ="/images/mkhalilov/flexio-builds/cc-host-bitmap-opt/apps/flexio_ag_bench/host/flexio_ag_bench"
    default_counter_binary_path             ="/images/mkhalilov/flexio-builds/cc-host-counter/apps/flexio_ag_bench/host/flexio_ag_bench"
    default_no_reliability_prof_binary_path ="/images/mkhalilov/flexio-builds/cc-host-no-rel-prof/apps/flexio_ag_bench/host/flexio_ag_bench"
    default_bitmap_prof_binary_path         ="/images/mkhalilov/flexio-builds/cc-host-prof/apps/flexio_ag_bench/host/flexio_ag_bench"
    default_bitmap_opt_prof_binary_path     ="/images/mkhalilov/flexio-builds/cc-host-bitmap-opt-prof/apps/flexio_ag_bench/host/flexio_ag_bench"
    default_counter_prof_binary_path        ="/images/mkhalilov/flexio-builds/cc-host-counter-prof/apps/flexio_ag_bench/host/flexio_ag_bench"

    def get_pe_num(ep_num):
        if ep_num <= 8:
            return int(1)
        elif ep_num <= 16:
            return int(2)
        else:
            return int(4)

    def __init__(self,
                 transport_name,
                 binary_opts,
                 binary_name=default_binary_name,
                 client_binary_path=default_binary_path,
                 server_binary_path=default_binary_path,
                 daemon_binary_path=default_binary_path,
                 client_dev='mlx5_0',
                 server_dev='mlx5_2',
                 daemon_dev='mlx5_2',
                 client_addr='10.245.43.1',
                 daemon_addr='10.245.43.20',
                 run_daemon=False,
                 max_chunk_size=4096,
                 max_threads_num=1,
                 get_pe_num=get_pe_num,
                 run_on_dpu=False):
        self.transport_name = transport_name
        self.binary_opts = binary_opts
        self.binary_name = binary_name
        self.server_binary_path = server_binary_path
        self.client_binary_path = client_binary_path
        self.daemon_binary_path = daemon_binary_path
        self.client_dev = client_dev
        self.server_dev = server_dev
        self.daemon_dev = daemon_dev
        self.client_addr = client_addr
        self.daemon_addr = daemon_addr
        self.run_daemon = run_daemon
        self.max_chunk_size = max_chunk_size
        self.max_threads_num = max_threads_num
        self.get_pe_num = get_pe_num
        self.run_on_dpu = run_on_dpu
        if (self.run_on_dpu):
            self.server_dev = 'mlx5_0' # make me cleaner

class MyExperiment:
    default_warmup = 1
    default_iters = 2
    default_timeout = 420 # 7 mins per run
    default_ssh_sleep_time = 5

    def __init__(self, 
                 transport,
                 msg_size, 
                 chunk_size, 
                 txw, 
                 qlen, 
                 threads_num,
                 iters=default_iters,
                 warmup=default_warmup,
                 timeout=default_timeout):
        self.transport   = transport
        self.msg_size    = msg_size
        self.chunk_size  = chunk_size
        self.txw         = txw
        self.qlen        = qlen
        self.threads_num = threads_num
        self.iters       = iters
        self.warmup      = warmup
        self.timeout     = timeout 


    def get_info_string(self):
        return f'{self.transport.transport_name}.{self.transport.binary_name}.{self.msg_size}.{self.chunk_size}.{self.txw}.{self.qlen}.{self.threads_num}'


    def skip(self):
        if (self.chunk_size > self.transport.max_chunk_size):
            return True
        if (self.threads_num > self.transport.max_threads_num):
            return True
        if (self.threads_num * self.chunk_size > self.msg_size):
            return True
        if (self.txw > self.qlen):
            return True
        if (self.warmup >= self.iters):
            return True
        return False


    def get_generic_cmd(self):
        cmd = [f'--payload_size={self.msg_size}',
               f'--chunk_size={self.chunk_size}',
               f'--tx_window={self.txw}',
               f'--epq_len={self.qlen}',
               f'--peq_len={self.qlen}',
               f'--iters={self.iters}',
               f'--warmup={self.warmup}']
        for opt in self.transport.binary_opts:
            cmd.append(opt)

        return cmd


    def set_server_cmd(self):
        self.server_cmd = []
        if not self.transport.run_on_dpu:
            self.server_cmd = ['taskset', '-c', '24']
        self.server_cmd.append(f'{self.transport.server_binary_path}')
        for opt in self.get_generic_cmd():
            self.server_cmd.append(opt)
        self.server_cmd.append(f'--rdma_dev={self.transport.server_dev}')
        self.server_cmd.append(f'--bmark_client_addr={self.transport.client_addr}')
        if self.transport.run_daemon:
            self.server_cmd.append(f'--hmem_daemon_addr={self.transport.daemon_addr}')
        self.server_cmd.append(f'--epn={self.threads_num}')
        self.server_cmd.append(f'--pen={self.threads_num}')


    def set_daemon_cmd(self):
        if self.transport.run_daemon:
            self.daemon_cmd = ['ssh', '-t', f'mkhalilov@{self.transport.daemon_addr}',
                               'taskset', '-c', '25', # client is always the host
                               f'{self.transport.daemon_binary_path}',
                               f'--rdma_dev={self.transport.daemon_dev}',
                               '--hmem_daemon']


    def set_client_cmd(self):
        self.client_cmd = ['ssh', '-t', f'mkhalilov@{self.transport.client_addr}',
                           'taskset', '-c', '24', # client is always the host
                           f'{self.transport.client_binary_path}']
        for opt in self.get_generic_cmd():
            self.client_cmd.append(opt)
        self.client_cmd.append(f'--rdma_dev={self.transport.client_dev}')
        self.client_cmd.append(f'--epn={self.threads_num}')
        self.client_cmd.append(f'--pen={self.transport.get_pe_num(self.threads_num)}')
        self.client_cmd.append('--log_per_iteration_time')

    def start(self):
        if self.transport.run_daemon:
            print(f'starting daemon: {self.daemon_cmd}\n')
            self.daemon_proc = subprocess.Popen(self.daemon_cmd, #shell=True,
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE)
            time.sleep(self.default_ssh_sleep_time)
        print(f'starting client: {self.client_cmd}\n')
        self.client_proc = subprocess.Popen(self.client_cmd, #shell=True,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)
        time.sleep(self.default_ssh_sleep_time)
        print(f'starting server: {self.server_cmd}\n')
        self.server_proc = subprocess.Popen(self.server_cmd, #shell=True,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)


    # dumb way to make sure that each new run starts without dead flexio processes from prev iter
    def cleanup_flexio(self, addr=""):
        kill_script = '/images/mkhalilov/flexio-builds/kill_all.sh'
        kill_cmd = ['ssh', '-t', f'mkhalilov@{addr}', 'sh']
        if addr != "":
            kill_cmd.append(kill_script)
        else:
            kill_cmd = ['sh', kill_script]
        subprocess.run(kill_cmd)

    def wait_completion(self):
        experiment_status = 0
        try:
            self.client_proc.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            print('experiment timed out')
            experiment_status = 1
        finally:
            self.client_proc.kill()
            self.server_proc.kill()
            if self.transport.run_daemon:
                self.daemon_proc.kill()
                self.cleanup_flexio(self.transport.daemon_addr)
            self.cleanup_flexio(self.transport.client_addr)
            self.cleanup_flexio()
            self.client_stdout, self.client_stderr = self.client_proc.communicate()
            self.server_stdout, self.server_stderr = self.server_proc.communicate()
            print(f'experiment completed with status={experiment_status}\n')


    def run(self):
        print(f'experiment running: {self.get_info_string()}\n')
        if self.skip():
            print('experiment skipped')
            return 1
        self.set_client_cmd()
        self.set_server_cmd()
        self.set_daemon_cmd()
        self.start()
        self.wait_completion()
        return 0


    def get_log_path(self, output_path):
        return f'{output_path}/{self.get_info_string()}.log'


    def dump_data(self, output_path):
        with open(self.get_log_path(output_path), 'w+') as output_file:
            output_file.write(f'#Experiment call arguments:\n')
            output_file.write(f'{self.client_cmd}\n')
            output_file.write(f'{self.server_cmd}\n')
            if self.transport.run_daemon:
                output_file.write(f'{self.daemon_cmd}\n')
            output_file.write(f'\n#Experiment server stderr: \n')
            output_file.write(self.server_stderr.decode())
            output_file.write(f'\n#Experiment server stdout: \n')
            output_file.write(self.server_stdout.decode())
            output_file.write(f'\n#Experiment client stderr: \n')
            output_file.write(self.client_stderr.decode())
            output_file.write(f'\n#Experiment client stdout: \n')
            output_file.write(self.client_stdout.decode())
        print(f'experiment data written to: {self.get_log_path(output_path)}\n')


def create_experiment_batch(transports, msg_sizes, chunk_sizes, txws, qlens, thread_nums, iters, warmup):
    experiment_batch = []
    for transport in transports:
        for msg_size in msg_sizes:
            for chunk_size in chunk_sizes:
                for txw in txws:
                    for qlen in qlens:
                        for threads_num in thread_nums:
                            experiment_batch.append(MyExperiment(transport=transport,
                                                                 msg_size=msg_size,
                                                                 chunk_size=chunk_size,
                                                                 txw=txw,
                                                                 qlen=qlen,
                                                                 threads_num=threads_num,
                                                                 iters=iters,
                                                                 warmup=warmup))
    return experiment_batch


def run_experiment_batch(experiment_batch, experiment_output_path):
    Path(experiment_output_path).mkdir(parents=True, exist_ok=True)
    cur_exp_id = 0
    for experiment in experiment_batch:
        # we use deepcopy to avoid buffering of all logged data after the experiment completed
        instance = copy.deepcopy(experiment)
        if not instance.run():
            instance.dump_data(experiment_output_path)
        cur_exp_id += 1
        print(f'Sub-experiment: {cur_exp_id}/{len(experiment_batch)}\n')


def main():
    assert len(sys.argv) == 3
    run_on_dpu = (sys.argv[1] == "1")
    if run_on_dpu: print('Experiment will be collected with a server on DPU\n')
    global_output_path = sys.argv[2]

    all_transports = [Transport(transport_name="uc_host2host", 
                                binary_opts=["--rnr_retry", "--transport=host"], 
                                max_chunk_size=65536,
                                run_on_dpu=run_on_dpu),
                      Transport(transport_name="uc_host2dpa", 
                                binary_opts=["--rnr_retry", "--transport=dpa"], 
                                max_chunk_size=65536, 
                                max_threads_num=128,
                                run_on_dpu=run_on_dpu),
                      Transport(transport_name="ud_host2dpa_staging_host",
                                binary_opts=["--rnr_retry", "--transport=dpa_dpu_proxy", "--staging_mem_type=host","--hmem_daemon_addr=10.245.43.20"], 
                                run_daemon=True,
                                max_chunk_size=4096,
                                max_threads_num=128,
                                run_on_dpu=run_on_dpu),
                      Transport(transport_name="ud_host2dpa_staging_dpa",
                                binary_opts=["--rnr_retry", "--transport=dpa_dpu_proxy", "--staging_mem_type=dpa","--hmem_daemon_addr=10.245.43.20"], 
                                run_daemon=True, 
                                max_chunk_size=4096, 
                                max_threads_num=128, 
                                run_on_dpu=run_on_dpu)]

    all_transports_no_rnr = copy.deepcopy(all_transports)
    for transport in all_transports_no_rnr:
        transport.binary_opts.remove('--rnr_retry')

    all_transports_prof = copy.deepcopy(all_transports)
    for transport in all_transports_prof:
        transport.server_binary_path = transport.default_prof_binary_path
        transport.binary_name = 'prof'

    default_txw                   = 128      # EMPIRICALLY FOUND OPTIMUM
    default_qlen                  = 8192     # MAX SQ/RQ LEN
    default_msg_size              = 8388608  # 8MB = not small, not large
    default_chunk_size            = 4096     # 4KB MTU

    default_iters_long = 5000
    default_warmup_long = 500
    default_iters_medium = 2500
    default_warmup_medium = 100
    default_iters_short = 500
    default_warmup_short = 100

    all_chunk_sizes               = [64,128,256,512,1024,4096,8192,16384,32768,65536]
    all_msg_sizes                 = [16384,65536,262144,1048576,4194304,8388608,16777216]
    all_txws                      = [1,2,4,8,16,32,64,128]
    all_qlens                     = [128,256,512,1024,2048,4096,8192]
    all_thread_nums               = [1,2,4,8,16,32,64,128]
    all_thread_nums_single_core   = [1,2,4,8,16]

    print(f'Experiment results will be written to {global_output_path}\n')

    print(f'Experiment: single-threaded txw')
    txw_tests_batch = create_experiment_batch(transports=all_transports[:-1], # skip DPA staging
                                              msg_sizes=[default_msg_size],
                                              chunk_sizes=[64,512,default_chunk_size],
                                              txws=all_txws,
                                              qlens=[default_qlen],
                                              thread_nums=[1],
                                              iters=default_iters_medium,
                                              warmup=default_warmup_medium)
    run_experiment_batch(txw_tests_batch, f"{global_output_path}/txw_logs")

    print(f'Experiment: single-threaded qlen')
    qlen_tests_batch = create_experiment_batch(transports=all_transports_no_rnr[1:-1], # skip host and DPA staging
                                               msg_sizes=[default_msg_size],
                                               chunk_sizes=[default_chunk_size],
                                               txws=[default_txw],
                                               qlens=all_qlens,
                                               thread_nums=all_thread_nums_single_core,
                                               iters=default_iters_short,
                                               warmup=default_warmup_short)
    run_experiment_batch(qlen_tests_batch, f"{global_output_path}/qlen_logs")

    print(f'Experiment: profiling 64/1MTU chunk sizes vs thread num')
    profiling_tests_batch = create_experiment_batch(transports=all_transports_prof[1:-1], # skip host and DPA staging
                                                    msg_sizes=[default_msg_size],
                                                    chunk_sizes=[64, 512, default_chunk_size],
                                                    txws=[default_txw],
                                                    qlens=[default_qlen],
                                                    thread_nums=all_thread_nums,
                                                    iters=default_iters_short,
                                                    warmup=default_warmup_short)
    run_experiment_batch(profiling_tests_batch, f"{global_output_path}/profling_logs")

    if not run_on_dpu:
        print(f'Experiment: single-threaded reliability')
        all_transports_reliability = [copy.deepcopy(all_transports[1]), # no reliability
                                      copy.deepcopy(all_transports[1]), # bitmap naive
                                      copy.deepcopy(all_transports[1]), # bitmap optimized
                                      copy.deepcopy(all_transports[1])] # counter
        all_transports_reliability[0].server_binary_path = transport.default_no_reliability_binary_path
        all_transports_reliability[0].binary_name = 'no_reliability'
        all_transports_reliability[1].server_binary_path = transport.default_bitmap_binary_path
        all_transports_reliability[1].binary_name = 'bitmap'
        all_transports_reliability[2].server_binary_path = transport.default_bitmap_opt_binary_path
        all_transports_reliability[2].binary_name = 'bitmap_optimized'
        all_transports_reliability[3].server_binary_path = transport.default_counter_binary_path
        all_transports_reliability[3].binary_name = 'counter'
        all_transports_reliability_prof = copy.deepcopy(all_transports_reliability)
        all_transports_reliability_prof[0].server_binary_path = transport.default_no_reliability_prof_binary_path
        all_transports_reliability_prof[1].server_binary_path = transport.default_bitmap_prof_binary_path
        all_transports_reliability_prof[2].server_binary_path = transport.default_bitmap_opt_prof_binary_path
        all_transports_reliability_prof[3].server_binary_path = transport.default_counter_prof_binary_path

        reliability_tests_batch = create_experiment_batch(transports=all_transports_reliability,
                                                          msg_sizes=[default_msg_size],
                                                          chunk_sizes=[64, 512, default_chunk_size],
                                                          txws=[default_txw],
                                                          qlens=[default_qlen],
                                                          thread_nums=[1],
                                                          iters=default_iters_medium,
                                                          warmup=default_warmup_medium)
        reliability_prof_tests_batch = create_experiment_batch(transports=all_transports_reliability_prof,
                                                               msg_sizes=[default_msg_size],
                                                               chunk_sizes=[64, 512, default_chunk_size],
                                                               txws=[default_txw],
                                                               qlens=[default_qlen],
                                                               thread_nums=[1],
                                                               iters=default_iters_short,
                                                               warmup=default_warmup_short)
        run_experiment_batch(reliability_tests_batch, f"{global_output_path}/reliability_logs")
        run_experiment_batch(reliability_prof_tests_batch, f"{global_output_path}/reliability_profling_logs")

    print(f'Experiment: message sizes vs thread num')
    msg_size_tests_batch = create_experiment_batch(transports=all_transports,
                                                   msg_sizes=all_msg_sizes,
                                                   chunk_sizes=[default_chunk_size],
                                                   txws=[default_txw],
                                                   qlens=[default_qlen],
                                                   thread_nums=all_thread_nums_single_core,
                                                   iters=default_iters_medium,
                                                   warmup=default_warmup_medium)
    run_experiment_batch(msg_size_tests_batch, f"{global_output_path}/msg_size_logs")

    print(f'Experiment: chunk sizes vs thread num')
    chunk_size_tests_batch = create_experiment_batch(transports=all_transports[:-1], # skip DPA staging
                                                     msg_sizes=[default_msg_size],
                                                     chunk_sizes=all_chunk_sizes,
                                                     txws=[default_txw],
                                                     qlens=[default_qlen],
                                                     thread_nums=all_thread_nums,
                                                     iters=default_iters_long,
                                                     warmup=default_warmup_long)
    run_experiment_batch(chunk_size_tests_batch, f"{global_output_path}/chunk_size_logs/")

if __name__ == '__main__':
    main()