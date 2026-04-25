#include <bits/stdc++.h>
#include <filesystem>
using namespace std;

static const int NUM_BUCKETS = 16; // keep file count under 20
static const char OP_INSERT = 'I';
static const char OP_DELETE = 'D';

static string data_dir = "kv_data_015"; // persistent across runs

static inline size_t bucket_id(const string &key) {
    return std::hash<string>{}(key) & (NUM_BUCKETS - 1);
}

static inline string bucket_path(size_t id) {
    char buf[64];
    snprintf(buf, sizeof(buf), "bucket_%02zu.log", id);
    return data_dir + "/" + string(buf);
}

static void ensure_data_dir() {
    static bool done = false;
    if (done) return;
    std::error_code ec;
    std::filesystem::create_directories(data_dir, ec);
    done = true;
}

static void append_record(char op, const string &key, int value) {
    ensure_data_dir();
    size_t bid = bucket_id(key);
    string path = bucket_path(bid);
    ofstream out(path, ios::app);
    out << op << ' ' << key << ' ' << value << '\n';
}

static void handle_find(const string &key) {
    ensure_data_dir();
    size_t bid = bucket_id(key);
    string path = bucket_path(bid);

    unordered_set<int> present;
    present.reserve(64);

    ifstream in(path);
    if (in) {
        string line;
        line.reserve(128);
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            char op = 0;
            string k;
            int v;
            istringstream iss(line);
            if (!(iss >> op >> k >> v)) continue;
            if (k == key) {
                if (op == OP_INSERT) {
                    present.insert(v);
                } else if (op == OP_DELETE) {
                    auto it = present.find(v);
                    if (it != present.end()) present.erase(it);
                }
            }
        }
    }

    if (present.empty()) {
        cout << "null\n";
        return;
    }

    vector<int> vals;
    vals.reserve(present.size());
    for (int v : present) vals.push_back(v);
    sort(vals.begin(), vals.end());

    for (size_t i = 0; i < vals.size(); ++i) {
        if (i) cout << ' ';
        cout << vals[i];
    }
    cout << '\n';
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
            string key; int value;
            cin >> key >> value;
            if (value < 0) continue;
            append_record(OP_INSERT, key, value);
        } else if (cmd == "delete") {
            string key; int value;
            cin >> key >> value;
            if (value < 0) continue;
            append_record(OP_DELETE, key, value);
        } else if (cmd == "find") {
            string key; cin >> key;
            handle_find(key);
        } else {
            string rest; getline(cin, rest);
        }
    }

    return 0;
}
