source env.sh
rm -rf data
mkdir -p data
gpinitsystem -c cluster.conf --lc-collate=C
createdb $USER
