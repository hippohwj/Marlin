//
// Created by Zhihan Guo on 3/31/23.
//

#include "OptionalGlobalData.h"
#include "Workload.h"
#include "local/ISchema.h"
#include "db/access/IndexBTreeObject.h"
#include "common/ConcurrentHashMap.h"

namespace arboretum {

void Workload::LoadSysTBL() {
  //TODO(Hippo): separate this into two functions
  //load systable and cache for it (being called in single thread context)
  LOG_INFO("Load system table");
  uint64_t max_key = ((uint64_t)config_->num_dataload_nodes_) * config_->num_rows_ - 1;
  size_t granule_num_per_node = GetGranuleID(config_->num_rows_ - 1) + 1;
  uint64_t max_granule_id = GetGranuleID(max_key);
  char data[schemas_[GRANULE_TABLE_NAME]->GetTupleSz()];

  for (uint64_t i = 0; i <= max_granule_id; i++) {
      uint64_t node_id = i / granule_num_per_node;      
      schemas_[GRANULE_TABLE_NAME]->SetPrimaryKey(static_cast<char *>(data),i);
      schemas_[GRANULE_TABLE_NAME]->SetNumericField(1, data, node_id);
      // LOG_INFO("insert into G_TBL with granule id %ld, node id %ld ", i, node_id);
      db_->InsertTuple(tables_[GRANULE_TABLE_NAME], i, data, schemas_[GRANULE_TABLE_NAME]->GetTupleSz());
      granule_tbl_cache->insert(static_cast<const uint64_t>(i), static_cast<const uint64_t>(node_id));
  }
  
  if (g_rampup_scaleout_enable) {
    // vector<vector<uint64_t>> node_granules_map;
    vector<uint64_t>* granules_node0 = new vector<uint64_t>();
    for (uint64_t i = 0; i <= max_granule_id; i++) {
      granules_node0->push_back(i);
    } 
    node_granules_map.push_back(granules_node0);
    
    if (g_node_id > 1 && g_node_id < g_client_node_id) {
      int granule_num_in_cluster = 32*12; 
      for (int dest_node = 1; dest_node < g_node_id; dest_node++) {
          vector<uint64_t>* granules_node= new vector<uint64_t>();
          int migr_granule_num_per_node = (granule_num_in_cluster/(dest_node + 1))/ dest_node; 
          for (int nodeid = 0; nodeid < dest_node; nodeid++) {
            for (uint64_t i = 0; i < migr_granule_num_per_node; i++) {
               uint64_t granule_id = node_granules_map[nodeid]->at(i);
               schemas_[GRANULE_TABLE_NAME]->SetPrimaryKey(static_cast<char *>(data),granule_id);
               schemas_[GRANULE_TABLE_NAME]->SetNumericField(1, data, dest_node);
               db_->InsertTuple(tables_[GRANULE_TABLE_NAME], granule_id, data, schemas_[GRANULE_TABLE_NAME]->GetTupleSz());
               granule_tbl_cache->insert(static_cast<const uint64_t>(granule_id), static_cast<const uint64_t>(dest_node));               
               granules_node->push_back(granule_id);
            }
          }
          node_granules_map.push_back(granules_node);
          for (int nodeid = 0; nodeid < dest_node; nodeid++) {
             auto v = node_granules_map[nodeid]; 
             v->erase(v->begin(),v->begin() + migr_granule_num_per_node);
          }
      }
    }
  }

  cout << "XXX: node_granules_map with size " << node_granules_map.size() << " is: ";
  for (int i = 0; i < node_granules_map.size(); i++) {
    cout << "[";
    for (int j = 0; j < node_granules_map[i]->size(); j++) {
      cout << node_granules_map[i]->at(j) << ",";
    }
    cout << "],";
  }
  cout << endl;
  cout<<"XXX: granule tbl initialized as : " << granule_tbl_cache->PrintStr() << endl;
}

void Workload::InitSchema(std::istream &in) {
  assert(sizeof(uint64_t) == 8);
  assert(sizeof(double) == 8);
  std::string line;
  ISchema *schema;
  uint32_t num_indexes = 0;
  uint32_t num_tables = 0;

  while (getline(in, line)) {
    if (line.compare(0, 6, "TABLE=") == 0) {
      std::string tname;
      tname = &line[6];
      getline(in, line);
      int col_count = 0;
      // Read all fields for this table.
      std::vector<std::string> lines;
      while (line.length() > 1) {
        lines.push_back(line);
        getline(in, line);
      }
      schema = new(MemoryAllocator::Alloc(sizeof(ISchema), 64)) ISchema(tname, lines.size());
      for (uint32_t i = 0; i < lines.size(); i++) {
        std::string line = lines[i];
        size_t pos = 0;
        std::string token;
        int elem_num = 0;
        int size = 0;
        bool is_pkey = false;
        // string type;
        DataType type;
        std::string name;
        while (line.length() != 0) {
          pos = line.find(',');
          if (pos == std::string::npos)
            pos = line.length();
          token = line.substr(0, pos);
          line.erase(0, pos + 1);
          switch (elem_num) {
            case 0: size = atoi(token.c_str());
              break;
            case 1: type = StringToDataType(token);
              break;
            case 2: name = token;
              break;
            case 3: is_pkey = atoi(token.c_str()) == 1;
              break;
            default: assert(false);
          }
          elem_num++;
        }
        assert(elem_num == 4);
        schema->AddCol(name, size, type, is_pkey);
        col_count++;
      }
      auto cur_tab = db_->CreateTable(tname, schema);
      num_tables++;
      tables_[tname] = cur_tab;
      schemas_[tname] = schema;
      LOG_INFO("table name is %s", tname.c_str());
      LOG_INFO("table size %d, schema size %d",tables_.size(), schemas_.size());
    } else if (line.compare(0, 6, "INDEX=")==0) {
      std::string iname;
      iname = &line[6];
      getline(in, line);
      std::vector<std::string> items;
      std::string token;
      size_t pos;
      while (line.length() != 0) {
        pos = line.find(',');
        if (pos == std::string::npos)
          pos = line.length();
        token = line.substr(0, pos);
        items.push_back(token);
        line.erase(0, pos + 1);
      }
      std::string tname(items[0]);
      OID table_id = tables_[tname];
      OID col_id = std::stoi(items[1]);
      if (g_index_type == IndexType::BTREE) {
        if (g_buf_type == BufferType::OBJBUF) {
          auto index = db_->CreateIndex(table_id, col_id, IndexType::BTREE);
          LOG_INFO("create index for table name %s", tname.c_str());
          indexes_.push_back(index);
        }
      }
    }
  }

  LOG_INFO("index size %d", indexes_.size());


}

} // arboretum
