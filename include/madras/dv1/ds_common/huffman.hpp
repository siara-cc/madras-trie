#ifndef _HUFFMAN_HPP_
#define _HUFFMAN_HPP_

namespace gen {

#ifndef UINTXX_WIDTH
#define UINTXX_WIDTH 32
#endif

#if UINTXX_WIDTH == 32
#define uintxx_t uint32_t
#define PRIuXX PRIu32
#else
#define uintxx_t uint64_t
#define PRIuXX PRIu64
#endif

template<class T>
class huffman {
  std::vector<uintxx_t> _freqs;
  struct entry {
    T freq;
    entry *parent;
    bool is_zero;
    entry(T _freq, entry *_parent, bool _is_zero) {
      freq = _freq;
      parent = _parent;
      is_zero = _is_zero;
    }
  };
  std::vector<entry *> _all_entries;
  entry *root;
  static bool greater(const entry *e1, const entry *e2) {
    return e1->freq > e2->freq;
  }
  void process_input() {
    for (size_t i = 0; i < _freqs.size(); i++) {
      _all_entries.push_back(new entry(_freqs[i], NULL, true));
    }
    std::vector<entry *> _entries = _all_entries;
    while (true) {
      std::sort(_entries.begin(), _entries.end(), greater);
      size_t last_idx = _entries.size() - 1;
      if (last_idx == 0)
        return;
      entry *e1 = _entries[last_idx--];
      entry *e2 = _entries[last_idx];
      _entries.resize(last_idx);
      entry *parent = new entry(e1->freq + e2->freq, NULL, true);
      e1->parent = parent;
      e1->is_zero = false;
      e2->parent = parent;
      e2->is_zero = true;
      _all_entries.push_back(parent);
      if (_entries.size() == 0) {
        root = parent;
        return;
      }
      _entries.push_back(parent);
    }
  }
  public:
    huffman(std::vector<uintxx_t> freqs) {
      _freqs = freqs;
      process_input();
    }
    ~huffman() {
      for (size_t i = 0; i < _all_entries.size(); i++)
        delete _all_entries[i];
    }
    uintxx_t get_code(int i, uintxx_t& len) {
      uintxx_t ret = 0;
      len = 0;
      entry *start = _all_entries[i];
      do {
        ret >>= 1;
        ret |= start->is_zero ? 0 : 0x80000000;
        len++;
        start = start->parent;
      } while (start != NULL && start->parent != NULL);
      ret >>= (32 - len);
      return ret;
    }
};

}

#endif
