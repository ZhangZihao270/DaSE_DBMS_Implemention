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
 2.validate whether the rset of this txn overlap with the wset in history list
 3.validate whether the rset and wset of this txn overlap with the wset in active list
 4.if have conflict, then abort and remove it from active
 5.if have no conflict, then apply write to the data
 6.remove itself from active and add it to history
*/
RC data_occ::validate(txn_man * txn)
{
	RC rc = RCOK;
	time_t start_ts = txn->start_ts;
	time_t ts = time(0);
	txn->end_ts = ts;
	time_t end_ts = txn->end_ts;
	vector<Data *> finsh_active;
	int f_active_len;
	bool valid = true;

	setEntry * wset=new setEntry();
	setEntry * rset=new setEntry();
	get_rw_set(txn, rset, wset);

	bool read_only = (wset->set_size == 0);
	setEntry * his;
	setEntry * ent;

	latch.lock();
	f_active_len = active_len;
	ent = active;
	if (!read_only) {
		active_len++;
		wset->next = active;
		active = wset;
	}
	his = history;
	latch.unlock();

	while (his != NULL) {
		bool overlap = is_overlap(his, start_ts, end_ts);
		if (overlap) {
			valid = test_validate(his, rset);
			if (!valid)
				goto final;
		}
		his = his->next;
	}

	while (ent != NULL) {
		valid = test_validate(ent, rset);
		if (!valid)
			goto final;
		valid = test_validate(ent, wset);
		if (!valid)
			goto final;
		ent = ent->next;
	}

	final:
	if (valid)
		rc = RCOK;
	else 
		rc = ABORT;

	free(rset);
	if (!read_only) {
		latch.lock();
		setEntry * act = active;
		setEntry * prev = NULL;
		while (act->txn != txn) {
			prev = act;
			act = act->next;
		}
		if (prev != NULL)
			prev->next = act->next;
		else
			active = act->next;
		active_len--;
		if (valid) {
			time_t ts = time(0);
			txn->commit_ts = ts;
			wset->next = history;
			history = wset;
			his_len++;

			for (int i = 0; i < wset->set_size; i++) {
				wset->datalist[i]->value = wset->values[i];
			}

		}
		latch.unlock();
	}
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
	//wset = (setEntry*)mem_allocator.alloc(sizeof(setEntry), 0);
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
	
	if (set->txn->commit_ts <= start)
		return false;
	else if (set->txn->commit_ts<=end)
		return true;
	/*
	if (set->txn->end_ts > start&&set->txn->end_ts <= end)
		return true;
	if (set->txn->start_ts < end&&set->txn->start_ts >= start)
		return true;
	if (set->txn->start_ts <= start && set->txn->end_ts >= end)
		return true;
	return false;
	*/
}
