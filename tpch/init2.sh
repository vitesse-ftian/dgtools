source env.sh
rm -rf data[012]
mkdir -p data{0,1,2}
gpinitsystem -c cluster.conf --lc-collate=C
createdb $USER
