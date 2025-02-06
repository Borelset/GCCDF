ChunkPath=/mnt/ssd-r/OdessHome/chunkFiles/%lu
LogicPath=/mnt/ssd-1/OdessHome/logicFiles/%lu
BatchFile=/mnt/nvme-1/$1.txt
RestorePath=/mnt/nvme-1/restoretest
LCGC=false
EnableRewriting=true
Rwriting=capping
CappingThres=20
GCInt=20
DelPerc=20
CacheLimit=100

LOGPATH=/home/zxy/GCCDF/build/log/$1/
RUNPATH=/home/zxy/GCCDF/build/Odess

echo "running $BatchFile"

$RUNPATH --task=batch --BatchFilePath=$BatchFile --ChunkFilePath=$ChunkPath --LogicFilePath=$LogicPath --RestorePath=$RestorePath --LCbasedGC=$LCGC --RewritingMethod=$Rwriting  --CappingThreshold=$CappingThres --enable_rewriting=$EnableRewriting --GCInterval=$GCInt --DeletePercentage=$DelPerc --CachingLimit=$CacheLimit > $LOGPATH/dedup.log  2>&1
