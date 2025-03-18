# HTC Dask Gateway
This branch holds a slight modification to the Kubernetes backend in order to support
the [HTC Dask Gateway](https://github.com/ncsa/htcdaskgateway). The requirement is for
the kubernetes backend to host just the Dask Scheduler and allow for workers on the
HTCondor cluster to join. 

We accomplish this by removing the code to launch worker pods.

We are publishing the resulting docker image as `ncsa/dask-gateway:2025.2.0-0.htc`

## Building Docker Image
The authors of the Dask Gateway package have a great Docker file for building the 
components. 