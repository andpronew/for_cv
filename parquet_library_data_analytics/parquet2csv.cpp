// parquet2csv.cpp
// Small helper: read a parquet file and dump CSV (semicolon-separated).
// Build:
//   g++ -std=gnu++23 -O3 parquet2csv.cpp -lparquet -larrow -lzstd -o parquet2csv
//
// Usage:
//   ./parquet2csv input.parquet > out.csv
//   ./parquet2csv input.parquet out.csv

#include <parquet/api/reader.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <memory>
#include <iomanip>

using namespace std;

static void fail(const string &msg)
{
    cerr << "ERROR: " << msg << "\n";
    exit(1);
}

int main(int argc, char** argv)
{
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " input.parquet [output.csv]\n";
        return 1;
    }

    const string infile = argv[1];
    const string outfile = (argc >= 3) ? argv[2] : string();

    ofstream fout;
    ostream* out = &cout;
    if (!outfile.empty()) {
        fout.open(outfile, ios::out);
        if (!fout.is_open()) fail("cannot open output file: " + outfile);
        out = &fout;
    }

    // Open parquet file (memory-mapped)
    unique_ptr<parquet::ParquetFileReader> reader;
    try {
        reader = parquet::ParquetFileReader::OpenFile(infile, /*memory_map=*/true);
    } catch (const exception& e) {
        fail(string("failed to open parquet: ") + e.what());
    }

    auto meta = reader->metadata();
    int n_cols = meta->schema()->num_columns();

    // collect column names
    vector<string> col_names;
    col_names.reserve(n_cols);
    for (int c = 0; c < n_cols; ++c) {
        col_names.push_back(meta->schema()->Column(c)->path()->ToDotString());
    }

    // print header (semicolon-separated)
    for (int c = 0; c < n_cols; ++c) {
        if (c) (*out) << ';';
        (*out) << col_names[c];
    }
    (*out) << '\n';

    // iterate row groups
    int num_row_groups = meta->num_row_groups();
    for (int rg = 0; rg < num_row_groups; ++rg) {
        auto rg_reader = reader->RowGroup(rg);
        int64_t rows = rg_reader->metadata()->num_rows();

        // For each column allocate storage according to physical type (INT64, BOOLEAN supported)
        vector< vector<int64_t> > ints(n_cols);       // store int64 columns (when applicable)
        vector< vector<char> > bools(n_cols);      // store boolean columns (0/1)
        vector<int> col_kind(n_cols, 0); // 1 = int64, 2 = bool, 0 = unsupported

        for (int c = 0; c < n_cols; ++c) {
            auto ptype = meta->schema()->Column(c)->physical_type();
            if (ptype == parquet::Type::INT64) {
                col_kind[c] = 1;
                ints[c].resize(rows);
                // read into ints[c]
                auto col = rg_reader->Column(c);
                auto* reader_i64 = static_cast<parquet::Int64Reader*>(col.get());
                int64_t done = 0;
                while (done < rows) {
                    int64_t values_read = 0;
                    int64_t levels = reader_i64->ReadBatch(rows - done,
                                                           nullptr, // def_levels
                                                           nullptr, // rep_levels
                                                           ints[c].data() + done,
                                                           &values_read);
                    if (levels == 0 && values_read == 0) break;
                    done += values_read;
                }
                if (done != rows) {
                    // if fewer values than expected, shrink
                    ints[c].resize(done);
                }
            } else if (ptype == parquet::Type::BOOLEAN) {
                col_kind[c] = 2;
                
bools[c].resize(rows);
auto col = rg_reader->Column(c);
auto* reader_bool = static_cast<parquet::BoolReader*>(col.get());
int64_t done = 0;
while (done < rows) {
    int64_t values_read = 0;
    int64_t levels = reader_bool->ReadBatch(rows - done,
                                           nullptr,
                                           nullptr,
                                           reinterpret_cast<bool*>(bools[c].data() + done),
                                           &values_read);
    if (levels == 0 && values_read == 0) break;
    done += values_read;
}

                if (done != rows) {
                    bools[c].resize(done);
                }
            } else {
                col_kind[c] = 0;
                // Unsupported physical type in this small tool: read as "unsupported"
                // We leave storage empty and will output placeholder per row.
            }
        }

        // Now print rows for this RG
        int64_t nrows_actual = rows;
        // if some columns shorter, clamp nrows_actual
        for (int c = 0; c < n_cols; ++c) {
            if (col_kind[c] == 1 && static_cast<int64_t>(ints[c].size()) < nrows_actual)
                nrows_actual = static_cast<int64_t>(ints[c].size());
            if (col_kind[c] == 2 && static_cast<int64_t>(bools[c].size()) < nrows_actual)
                nrows_actual = static_cast<int64_t>(bools[c].size());
        }

        for (int64_t r = 0; r < nrows_actual; ++r) {
            for (int c = 0; c < n_cols; ++c) {
                if (c) (*out) << ';';
                if (col_kind[c] == 1) {
                    (*out) << ints[c][r];
                } else if (col_kind[c] == 2) {
                    (*out) << (bools[c][r] ? "True" : "False");
                } else {
                    // placeholder: unsupported column type
                    (*out) << "";
                }
            }
            (*out) << '\n';
        }
    }

    if (fout.is_open()) fout.close();
    return 0;
}

