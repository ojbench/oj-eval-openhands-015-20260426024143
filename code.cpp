#include <bits/stdc++.h>
#include <filesystem>
using namespace std;

// Design: 16 bucket files (<=20 file limit). Each bucket is a binary file
// with a header and a fixed-size open-addressing hash table mapping
// 64-bit key hash -> file offset of latest snapshot for that key.
// Snapshot record stores key (to verify) and sorted values.
// This gives O(1) average-time find/insert/delete without scanning whole bucket.

static const int NUM_BUCKETS = 16; // keep file count under 20
static const uint64_t TABLE_SLOTS = 8192; // per-bucket index slots (load factor ~0.76 max)

static const uint64_t SLOT_SIZE = 16; // 8 bytes hash, 8 bytes offset
static const uint32_t FILE_VERSION = 1;
static const char FILE_MAGIC[8] = {'K','V','1','5','I','D','X','\0'};

static const uint32_t REC_MAGIC = 0xABCD1234; // snapshot record magic

static string data_dir = "kv_data_015"; // persistent across runs

static inline size_t bucket_id(const string &key) {
    return std::hash<string>{}(key) & (NUM_BUCKETS - 1);
}

static inline string bucket_path(size_t id) {
    char buf[64];
    snprintf(buf, sizeof(buf), "bucket_%02zu.bin", id);
    return data_dir + "/" + string(buf);
}

struct BucketHeader {
    char magic[8];
    uint32_t version;
    uint32_t reserved;
    uint64_t table_slots;
    uint64_t table_offset;
    uint64_t append_offset;
};

static void ensure_data_dir() {
    static bool done = false;
    if (done) return;
    std::error_code ec;
    std::filesystem::create_directories(data_dir, ec);
    done = true;
}

static void init_bucket(fstream &fs, const string &path) {
    // Create new file with header and zeroed table
    fs.open(path, ios::in | ios::out | ios::binary | ios::trunc);
    BucketHeader hdr{};
    memcpy(hdr.magic, FILE_MAGIC, 8);
    hdr.version = FILE_VERSION;
    hdr.reserved = 0;
    hdr.table_slots = TABLE_SLOTS;
    hdr.table_offset = sizeof(BucketHeader);
    hdr.append_offset = hdr.table_offset + TABLE_SLOTS * SLOT_SIZE;
    fs.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
    // zero table
    vector<char> zero(SLOT_SIZE * TABLE_SLOTS, 0);
    fs.write(zero.data(), zero.size());
    fs.flush();
}

static bool open_bucket(const string &path, fstream &fs, BucketHeader &hdr) {
    // Ensure file exists and valid; otherwise initialize
    if (!std::filesystem::exists(path)) {
        init_bucket(fs, path);
    } else {
        fs.open(path, ios::in | ios::out | ios::binary);
        if (!fs) {
            // try create anew
            fs.clear();
            init_bucket(fs, path);
        } else {
            fs.seekg(0, ios::beg);
            fs.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
            if (!fs || memcmp(hdr.magic, FILE_MAGIC, 8) != 0 || hdr.version != FILE_VERSION || hdr.table_slots != TABLE_SLOTS) {
                // reinitialize incompatible file
                fs.close();
                init_bucket(fs, path);
            } else {
                return true;
            }
        }
    }
    // After init, read header back
    fs.seekg(0, ios::beg);
    fs.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
    return true;
}

static inline uint64_t key_hash64(const string &s) {
    return static_cast<uint64_t>(std::hash<string>{}(s));
}

static bool read_snapshot_at(fstream &fs, uint64_t offset, string &key, vector<int> &vals) {
    if (offset == 0) return false;
    fs.seekg(offset, ios::beg);
    uint32_t magic = 0;
    uint32_t key_len = 0;
    uint32_t cnt = 0;
    fs.read(reinterpret_cast<char*>(&magic), sizeof(magic));
    if (!fs || magic != REC_MAGIC) return false;
    fs.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
    if (!fs) return false;
    string k;
    k.resize(key_len);
    fs.read(&k[0], key_len);
    if (!fs) return false;
    fs.read(reinterpret_cast<char*>(&cnt), sizeof(cnt));
    if (!fs) return false;
    vector<int> tmp;
    tmp.resize(cnt);
    if (cnt) fs.read(reinterpret_cast<char*>(tmp.data()), cnt * sizeof(int));
    if (!fs) return false;
    key.swap(k);
    vals.swap(tmp);
    return true;
}

static uint64_t write_snapshot_and_get_offset(fstream &fs, BucketHeader &hdr, const string &key, const vector<int> &vals) {
    fs.seekp(hdr.append_offset, ios::beg);
    uint32_t magic = REC_MAGIC;
    uint32_t key_len = static_cast<uint32_t>(key.size());
    uint32_t cnt = static_cast<uint32_t>(vals.size());
    fs.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
    fs.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
    fs.write(key.data(), key_len);
    fs.write(reinterpret_cast<const char*>(&cnt), sizeof(cnt));
    if (cnt) fs.write(reinterpret_cast<const char*>(vals.data()), cnt * sizeof(int));
    uint64_t offset = hdr.append_offset;
    hdr.append_offset = static_cast<uint64_t>(fs.tellp());
    // persist updated header append_offset
    fs.seekp(offsetof(BucketHeader, append_offset), ios::beg);
    fs.write(reinterpret_cast<const char*>(&hdr.append_offset), sizeof(hdr.append_offset));
    fs.flush();
    return offset;
}

struct SlotEntry { uint64_t h; uint64_t off; };

static bool read_slot(fstream &fs, const BucketHeader &hdr, uint64_t idx, SlotEntry &e) {
    uint64_t pos = hdr.table_offset + idx * SLOT_SIZE;
    fs.seekg(pos, ios::beg);
    fs.read(reinterpret_cast<char*>(&e.h), sizeof(uint64_t));
    fs.read(reinterpret_cast<char*>(&e.off), sizeof(uint64_t));
    return (bool)fs;
}

static void write_slot(fstream &fs, const BucketHeader &hdr, uint64_t idx, uint64_t h, uint64_t off) {
    uint64_t pos = hdr.table_offset + idx * SLOT_SIZE;
    fs.seekp(pos, ios::beg);
    fs.write(reinterpret_cast<const char*>(&h), sizeof(uint64_t));
    fs.write(reinterpret_cast<const char*>(&off), sizeof(uint64_t));
}

static bool lookup_key(fstream &fs, const BucketHeader &hdr, const string &key, uint64_t &slot_idx, uint64_t &offset, bool for_insert) {
    uint64_t h = key_hash64(key);
    uint64_t start = h % hdr.table_slots;
    int first_empty = -1;
    for (uint64_t i = 0; i < hdr.table_slots; ++i) {
        uint64_t idx = (start + i) & (hdr.table_slots - 1); // hdr.table_slots is power-of-two? 8192 yes
        SlotEntry e{};
        fs.clear();
        if (!read_slot(fs, hdr, idx, e)) return false;
        if (e.h == 0) {
            if (first_empty < 0) first_empty = (int)idx;
            break; // empty, not found
        }
        if (e.h == h) {
            // verify snapshot key
            string k; vector<int> vals;
            streampos oldposg = fs.tellg();
            if (read_snapshot_at(fs, e.off, k, vals) && k == key) {
                slot_idx = idx; offset = e.off; return true;
            }
            fs.clear();
            fs.seekg(oldposg);
            // hash collision with different key, continue probing
        }
    }
    if (for_insert) {
        if (first_empty < 0) return false; // table full
        slot_idx = (uint64_t)first_empty; offset = 0; return false;
    }
    return false;
}

static void bucket_find(const string &key) {
    ensure_data_dir();
    size_t bid = bucket_id(key);
    string path = bucket_path(bid);
    fstream fs;
    BucketHeader hdr{};
    open_bucket(path, fs, hdr);

    uint64_t slot_idx = 0, off = 0;
    bool found = lookup_key(fs, hdr, key, slot_idx, off, false);
    if (!found || off == 0) { cout << "null\n"; return; }
    string k; vector<int> vals;
    fs.clear();
    if (!read_snapshot_at(fs, off, k, vals) || k != key || vals.empty()) { cout << "null\n"; return; }
    // vals stored sorted
    for (size_t i = 0; i < vals.size(); ++i) {
        if (i) cout << ' ';
        cout << vals[i];
    }
    cout << '\n';
}

static void bucket_update(const string &key, int value, bool is_insert) {
    ensure_data_dir();
    size_t bid = bucket_id(key);
    string path = bucket_path(bid);
    fstream fs;
    BucketHeader hdr{};
    open_bucket(path, fs, hdr);

    uint64_t slot_idx = 0, off = 0;
    bool exists = lookup_key(fs, hdr, key, slot_idx, off, true);

    vector<int> vals;
    if (exists && off) {
        string k; vector<int> tmp;
        fs.clear();
        if (read_snapshot_at(fs, off, k, tmp) && k == key) {
            vals.swap(tmp);
        }
    }
    // Update vals (keep sorted, unique)
    bool changed = false;
    if (is_insert) {
        auto it = lower_bound(vals.begin(), vals.end(), value);
        if (it == vals.end() || *it != value) { vals.insert(it, value); changed = true; }
    } else {
        auto it = lower_bound(vals.begin(), vals.end(), value);
        if (it != vals.end() && *it == value) { vals.erase(it); changed = true; }
    }
    if (!changed) return;

    // Append new snapshot and update slot
    fs.clear();
    uint64_t new_off = write_snapshot_and_get_offset(fs, hdr, key, vals);
    // refresh header after write (append_offset moved)
    fs.seekg(0, ios::beg); fs.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));

    uint64_t h = key_hash64(key);
    if (!exists) {
        // slot_idx was set to first empty in lookup_key(for_insert=true)
        write_slot(fs, hdr, slot_idx, h, new_off);
    } else {
        write_slot(fs, hdr, slot_idx, h, new_off);
    }
    fs.flush();
}

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    int n;
    if (!(cin >> n)) return 0;

    ensure_data_dir();

    for (int i = 0; i < n; ++i) {
        string cmd;
        cin >> cmd;
        if (cmd == "insert") {
            string key; int value; cin >> key >> value; if (value < 0) continue;
            bucket_update(key, value, true);
        } else if (cmd == "delete") {
            string key; int value; cin >> key >> value; if (value < 0) continue;
            bucket_update(key, value, false);
        } else if (cmd == "find") {
            string key; cin >> key; bucket_find(key);
        } else {
            string rest; getline(cin, rest);
        }
    }
    return 0;
}
