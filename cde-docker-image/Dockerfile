FROM container.repository.cloudera.com/cloudera/dex/dex-spark-runtime-3.1.1:1.7.0-b129
USER root
RUN dnf install -y git gcc gcc-c++ make && dnf clean all && rm -rf /var/cache/dnf
RUN pip3 install --upgrade cython setuptools
RUN pip3 install pandas wheel scoring

# need to manually compile libspatialindex
RUN mkdir repos && \
    cd repos && \
    git clone https://github.com/libspatialindex/libspatialindex.git && \
    cd libspatialindex && \
    cmake -DCMAKE_INSTALL_PREFIX=/usr . && \
    make -j4 && \
    make install && \
    cd ..

RUN dnf install -y automake libtool sqlite-devel diffutils libtiff-devel libcurl-devel

# install proj
RUN cd repos && \
    git clone https://github.com/OSGeo/PROJ.git && \
    cd PROJ && \
    ./autogen.sh && \
    autoreconf -fvi && \
    ./configure --prefix=/usr 

RUN cd repos/PROJ && \
    make -j$(nproc) && \
    make install

# need to install spatialtools

RUN pip3 install geopandas rtree
USER 1345