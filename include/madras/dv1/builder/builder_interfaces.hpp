#ifndef __DV1_BUILDER_INTERFACES__
#define __DV1_BUILDER_INTERFACES__

#include "output_writer.hpp"
#include "madras/dv1/common.hpp"

namespace madras { namespace dv1 {

struct uniq_info_base {
  uintxx_t pos;
  uintxx_t len;
  uintxx_t arr_idx;
  uintxx_t freq_count;
  union {
    uintxx_t ptr;
    uintxx_t repeat_freq;
  };
  uint8_t grp_no;
  uint8_t flags;
};

struct uniq_info : public uniq_info_base {
  uintxx_t link_arr_idx;
  uintxx_t cmp;
  uintxx_t cmp_min;
  uintxx_t cmp_max;
  uniq_info(uintxx_t _pos, uintxx_t _len, uintxx_t _arr_idx, uintxx_t _freq_count) {
    memset(this, '\0', sizeof(*this));
    pos = _pos; len = _len;
    arr_idx = _arr_idx;
    cmp_min = UINTXX_MAX;
    freq_count = _freq_count;
    link_arr_idx = UINTXX_MAX;
  }
};
typedef std::vector<uniq_info_base *> uniq_info_vec;

struct freq_grp {
  uintxx_t grp_no;
  uintxx_t grp_log2;
  uintxx_t grp_limit;
  uintxx_t count;
  uintxx_t freq_count;
  uintxx_t grp_size;
  uint8_t code;
  uint8_t code_len;
};

struct node_data {
  uint8_t *data;
  uintxx_t len;
  uintxx_t ns_id;
  uint8_t node_idx;
  uint8_t offset;
};
typedef std::vector<node_data> node_data_vec;

class sort_callbacks {
  public:
    virtual uint8_t *get_data_and_len(memtrie::node& n, uintxx_t& len, char type = '*') = 0;
    virtual void set_uniq_pos(uintxx_t ns_id, uint8_t node_idx, size_t pos) = 0;
    virtual int compare(const uint8_t *v1, int len1, const uint8_t *v2, int len2, int trie_level) = 0;
    virtual void sort_data(node_data_vec& nodes_for_sort, int trie_level) = 0;
    virtual ~sort_callbacks() {
    }
};

class trie_builder_fwd {
  public:
    output_writer output;
    bldr_options *opts = nullptr;
    uint16_t pk_col_count;
    uint16_t trie_level;
    trie_builder_fwd(const bldr_options *_opts, uint16_t _trie_level, uint16_t _pk_col_count)
      : trie_level (_trie_level), pk_col_count (_pk_col_count) {
      opts = new bldr_options[_opts->opts_count];
      memcpy(opts, _opts, sizeof(bldr_options) * _opts->opts_count);
    }
    virtual ~trie_builder_fwd() {
    }
    void set_fp(FILE *fp) {
      output.set_fp(fp);
    }
    void set_out_vec(byte_vec *out_vec) {
      output.set_out_vec(out_vec);
    }
    virtual memtrie::in_mem_trie *get_memtrie() = 0;
    virtual trie_builder_fwd *new_instance() = 0;
    virtual memtrie::node_set_vars insert(const uint8_t *key, int key_len, uintxx_t val_pos = UINTXX_MAX) = 0;
    virtual uintxx_t build() = 0;
    virtual void write_trie() = 0;
    virtual bldr_options *get_opts() = 0;
};

}}

#endif
