#include "data_occ.h"

data_occ::data_occ()
{
	his_len = 0;
	active_len = 0;
	history = NULL;
	active = NULL;
}

void data_occ::init()
{
	his_len = 0;
	active_len = 0;
	history = NULL;
	active = NULL;
}
/*
 1.get end_ts, active txn list, history txn list, and add itself to active list
 2.validate if the rset of this txn overlap with the wset in history list
 3.validate if the rset and wset of this txn overlap with the wset in active list
 4.if have conflict, then abort and remove it from active
 5.if have no conflict, then apply write to the data
 6.remove itself from active and add it to history
*/
RC data_occ::validate(txn_man * txn)
{
	RC rc = RCOK;
	
	return rc;
}

/*
test if two set are confilct
*/
bool data_occ::test_validate(setEntry * set1, setEntry * set2)
{
	if (set1->txn->tid == set2->txn->tid)
		return true;
	for (int i = 0; i < set1->set_size; i++)
		for (int j = 0; j < set2->set_size; j++) {
			if (set1->datalist[i] == set2->datalist[j]) {
				return false;
			}
		}
	return true;
}

/*
get the rset and wset of a txn
*/
RC data_occ::get_rw_set(txn_man * txn, setEntry * &rset, setEntry * &wset)
{
	wset->set_size = txn->wr_cnt;
	rset->set_size = txn->rd_cnt;
	wset->txn = txn;
	rset->txn = txn;
	for (int i = 0; i < txn->wr_cnt; i++) {
		wset->datalist.push_back(txn->wr_list[i]->data);
		wset->values.push_back(txn->wr_list[i]->value);
	}
	for (int j = 0; j < txn->rd_cnt; j++) {
		rset->datalist.push_back(txn->rd_list[j]);
	}
	return RCOK;
}

/*
test if a txn is overlap with this time scope
*/
bool data_occ::is_overlap(setEntry * set, time_t start, time_t end)
{
	
	return false;
}
