DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/dghome/greenplum_path.sh


# NOTE: DO NOT SET PGPORT!!!  
# We need to allow user to specify this in their .profile or .bashrc
# for their respective environments
# export PGPORT=5432
export MASTER_DATA_DIRECTORY=$DIR/data/dg-1
