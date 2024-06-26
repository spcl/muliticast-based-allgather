# Bandwidth-Optimal, Fully-Offloaded Collectives

The paper will be presented at the SC '24 (The International Conference for High Performance Computing, Networking, Storage, and Analysis).

## Artifact structure

 - CPU-driven Multicast-based Allgather/Broadcast integrated within the UCC library:
    - `./ucc/src/components/tl/spin` - collectives backend source-code.
    - `./osu-micro-benchmarks-7.3` - OSU MPI benchmarks supporting per-iteration timing for Allgather/Broadcast collectives
    - `./ompi/` and `./ucx`- subrepos with project dependencies 
 - DPA-offloaded collective receive datapath PoC:
    - `./coll-offloading/*`
    - `./coll-offloading/README.md` - detailed manual for building/running PoC.
 - Multicast-based Allgather traffic model:
    - `./sim/estimate_allgather_cost.py`

## Building and running UCC-based collectives backend

**Prerequisites**: InfiniBand-based cluster and user permissions to create multicast-groups through OpenSM.

1) `$ git submodule update --init --recursive`
2) `$ bash ./build.sh`
3) `$ source ./sourceme.sh`
4) `$ cd ./osu-micro-benchmarks-7.3/ && ./configure CC=mpicc CXX=mpicxx && make -j`

OpenMPI 5.0.3 compiled with the support of UCC multicast backend will be stored in `./ompi/build`. Multicast-based backend can be tested by running:

`mpirun -x LD_LIBRARY_PATH -x PATH --hostfile <machine_hostfile> -np <nprocs> --mca coll_ucc_enable 1 --mca coll_ucc_priority 100 ./osu-micro-benchmarks-7.3/c/mpi/collective/blocking/osu_allgather -m 4096:8388608`

## DPA offloading

**Prerequisites**: Two servers with BlueField-3 NIC and DOCA v2.2.0.

See `./coll-offloading/README.md` for building and running instructions instructions.

## Traffic reduction with Multicast

Run `python3 ./sim/estimate_allgather_cost.py` to get .csv output with traffic estimations for different Allgather algorithms.
