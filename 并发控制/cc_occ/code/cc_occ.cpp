#include "cc_occ.h"

//写操作不立即更新到数据上，而是先保留在事务的写集合中
RC cc_occ::update(string key, int value, txn_man * txn)
{
	RC rc = RCOK;
	//Data data = Data();
	Data* data = new Data();
	data->value = value;
	auto find = engine.data_map.find(key);
	if (find != engine.data_map.end() && !find->second.deleted) {
		updateEntry * updateentry = new updateEntry();
		updateentry->data = &(find->second);
		updateentry->value = value;
		txn->wr_list.push_back(updateentry);
		txn->wr_cnt++;
	}
	return rc;
}

//读操作记录在事务的读集合中
RC cc_occ::get(string key, int & value, txn_man * txn)
{
	RC rc = RCOK;

	auto find = engine.data_map.find(key);
	if (find != engine.data_map.end() && !find->second.deleted) {
		value = find->second.value;
		txn->rd_list.push_back(&(find->second));
		txn->rd_cnt++;
	}
	return rc;
}

//对事务进行提交
RC cc_occ::commit(txn_man * txn)
{
	RC rc = RCOK;

	//提交时进行事务验证
	rc = occ_man.validate(txn);
	return rc;
}