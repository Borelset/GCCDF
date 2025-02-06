ChunkPath=/mnt/ssd-r/OdessHome/chunkFiles/%lu
LogicPath=/mnt/ssd-1/OdessHome/logicFiles/%lu
BatchFile=/mnt/nvme-1/$1.txt
RestorePath=/mnt/nvme-1/restoretest
LCGC=false
EnableRewriting=true
Rwriting=capping
CappingThres=10
GCInt=4
DelPerc=20
CacheLimit=100

LOGPATH=/home/zxy/GCCDF/build/log/$1/
RUNPATH=/home/zxy/GCCDF/build/Odess

echo "running $BatchFile"

for ((i=0; i < $2; i++)); do
    printf -v paddedi "%03d" $i  # 将数字补齐为3位数
    echo "Restoring # $paddedi"
    echo "123567" | sudo -S sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    $RUNPATH --task=get --ChunkFilePath=$ChunkPath --LogicFilePath=$LogicPath --RestorePath=$RestorePath --RecipeID=$i > $LOGPATH/restore_$paddedi.log  2>&1

done
