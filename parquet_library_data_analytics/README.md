g++ -std=gnu++23 -O3 -DNDEBUG -march=native -mtune=native -flto=auto -fno-plt parquet_reader.cpp parquet_reader_lib.cpp -lparquet -larrow -lzstd -o parquet_reader -g
