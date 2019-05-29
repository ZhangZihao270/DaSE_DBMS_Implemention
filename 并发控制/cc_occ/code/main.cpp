#include<iostream>
#include<string>
#include<queue>
#include<vector>
#include<thread>
#include<ctime>
#include<Windows.h>
#include"cc_lock.h"
#include"cc_to.h"
#include"cc_occ.h"
#include"data_to.h"
#include"global.h"
#include"structure.h"
#include"txn.h"
#include"data.h"

using namespace std;

struct Lock {
	string key;
	lock_t type;
	Lock(string k, lock_t t) {
		key = k;
		type = t;
	}
};

void initial_data(Engine& engine, vector<int> &datalist);

RC do_transaction1(cc_occ& ccOCC);
RC do_transaction2(cc_occ& ccOCC);
RC do_transaction3(cc_occ& ccOCC);
RC transaction1(cc_occ& ccOCC);
RC transaction2(cc_occ& ccOCC);
RC transaction3(cc_occ& ccOCC);
RC transaction4(cc_occ& ccOCC);
//RC release_lock(cc_lock& ccLock, vector<Lock> getLockList, thread::id tid);
time_t get_ts();

vector<int> datalist;
mutex latch;

int main() {
	//Engine engine = Engine(CC_OCC);
	cc_occ ccOCC = cc_occ();
	vector<std::thread> threads;

	//initial data_map
	initial_data(ccOCC.engine, datalist);
	
	for (int i = 0; i < 9; i++) {
		int j = i % 3;
		if (j == 0) {
			threads.push_back(std::thread(do_transaction1, std::ref(ccOCC)));
		}
		else if (j == 1) {
			threads.push_back(std::thread(do_transaction2, std::ref(ccOCC)));
		}
		else if (j == 2) {
			threads.push_back(std::thread(do_transaction3, std::ref(ccOCC)));
		}
		/*
		else if (j == 3) {
			threads.push_back(std::thread(transaction4, std::ref(ccLock)));
		}*/
	}
	for (auto& th : threads) {
		th.join();
	}

	for (int i = 0; i < datalist.size(); i++) {
		auto find = ccOCC.engine.data_map.find(to_string(datalist[i]));
		if (find->second.deleted) {
			cout << "Key: " << i << "   has been deleted "  << endl; 
		}
		cout << "Key: " << i << "   Value: " << find->second.value << endl;
		//cout << find->second.owner.type;
	}
	system("pause");
	return 0;
}

void initial_data(Engine& engine, vector<int> &datalist) {
	for (int i = 0; i < 6; i++) {
		//data_to data = data_to();
		//data.data_.value = i+100;
		Data data = Data();
		data.value = i + 100;
		engine.data_map[to_string(i)] = data;
		datalist.push_back(i);
	}
}

RC do_transaction1(cc_occ& ccOCC) {
	RC rc = RCOK;
	rc = transaction1(ccOCC);
	if (rc == ABORT)
		cout << "thread: " << this_thread::get_id() << " do txn1 is abort" << endl;
	while (rc != RCOK) {
		//Sleep(100);
		rc = transaction1(ccOCC);
	}
	return rc;
}

RC do_transaction2(cc_occ& ccOCC) {
	RC rc = RCOK;
	rc = transaction2(ccOCC);
	if (rc == ABORT)
		cout << "thread: " << this_thread::get_id() << " do txn2 is abort" << endl;
	while (rc != RCOK) {
		//Sleep(100);
		rc = transaction2(ccOCC);
	}
	return rc;
}

RC do_transaction3(cc_occ& ccOCC) {
	RC rc = RCOK;
	rc = transaction3(ccOCC);
	if (rc == ABORT)
		cout << "thread: " << this_thread::get_id() << " do txn3 is abort" << endl;
	while (rc != RCOK) {
		//Sleep(100);
		rc = transaction3(ccOCC);
	}
	return rc;
}

RC transaction1(cc_occ& ccOCC) {
	RC rc = RCOK;
	txn_man * txn = new txn_man();
	thread::id tid = this_thread::get_id();
	time_t start_ts = get_ts();
	txn->tid = tid;
	txn->start_ts = start_ts;
	int k1 = 0, k2 = 2;
	int v1, v2;
	rc = ccOCC.get(to_string(k1), v1, txn);
	if (rc != RCOK)
		return rc;
	rc = ccOCC.get(to_string(k2), v2, txn);
	if (rc != RCOK)
		return rc;
	if (v1 >= 2) {
		rc = ccOCC.update(to_string(k1), v1 - 2, txn);
		if (rc != RCOK)
			return rc;
		rc = ccOCC.update(to_string(k2), v2 + 2, txn);
		if (rc != RCOK)
			return rc;
	}
	time_t end_ts = get_ts();
	txn->end_ts = end_ts;
	rc = ccOCC.commit(txn);
	return rc;
}

RC transaction2(cc_occ& ccOCC)
{
	RC rc = RCOK;
	txn_man * txn = new txn_man();
	thread::id tid = this_thread::get_id();
	time_t start_ts = get_ts();
	txn->tid = tid;
	txn->start_ts = start_ts;
	int k1 = 0, k2 = 4;
	int v1, v2;
	rc = ccOCC.get(to_string(k1), v1, txn);
	if (rc != RCOK)
		return rc;
	rc = ccOCC.get(to_string(k2), v2, txn);
	if (rc != RCOK)
		return rc;
	if (v1 >= 2) {
		rc = ccOCC.update(to_string(k1), v1 - 2, txn);
		if (rc != RCOK)
			return rc;
		rc = ccOCC.update(to_string(k2), v2 + 2, txn);
		if (rc != RCOK)
			return rc;
	}
	time_t end_ts = get_ts();
	txn->end_ts = end_ts;
	rc = ccOCC.commit(txn);
	return rc;
}

RC transaction3(cc_occ& ccOCC)
{
	RC rc = RCOK;
	txn_man * txn = new txn_man();
	thread::id tid = this_thread::get_id();
	time_t start_ts = get_ts();
	txn->tid = tid;
	txn->start_ts = start_ts;
	int k1 = 1, k2 = 3, k3 = 5;
	int v1, v2, v3;
	rc = ccOCC.get(to_string(k1), v1, txn);
	if (rc != RCOK)
		return rc;
	rc = ccOCC.get(to_string(k2), v2, txn);
	if (rc != RCOK)
		return rc;
	rc = ccOCC.get(to_string(k3), v3, txn);
	if (rc != RCOK)
		return rc;
	rc = ccOCC.update(to_string(k1), v1 * 2, txn);
	if (rc != RCOK)
		return rc;
	rc = ccOCC.update(to_string(k2), v2 + 5, txn);
	if (rc != RCOK)
		return rc;
	rc = ccOCC.update(to_string(k3), v3 + 10, txn);
	if (rc != RCOK)
		return rc;
	time_t end_ts = get_ts();
	txn->end_ts = end_ts;
	rc = ccOCC.commit(txn);
	return rc;
}
/*
RC transaction2(cc_to& ccTO)
{
	RC rc = RCOK;
	thread::id tid = this_thread::get_id();
	time_t ts = get_ts();
	int k1 = 0, k2 = 4;
	int v1, v2;
	rc = ccTO.get(to_string(k1), v1, tid, ts);
	if (rc != RCOK)
		return rc;
	rc = ccTO.get(to_string(k2), v2, tid, ts);
	if (rc != RCOK)
		return rc;
	if (v1 >= 2) {
		rc = ccTO.update(to_string(k1), v1 - 2, tid, ts);
		if (rc != RCOK)
			return rc;
		rc = ccTO.update(to_string(k2), v2 + 2, tid, ts);
		if (rc != RCOK)
			return rc;
	}
	return rc;
}

RC transaction3(cc_to& ccTO)
{
	RC rc = RCOK;
	thread::id tid = this_thread::get_id();
	time_t ts = get_ts();
	int k1 = 1, k2 = 3, k3=5 ;
	int v1, v2, v3;
	rc = ccTO.get(to_string(k1), v1, tid, ts);
	if (rc != RCOK)
		return rc;
	rc = ccTO.get(to_string(k2), v2, tid, ts);
	if (rc != RCOK)
		return rc;
	rc = ccTO.get(to_string(k3), v3, tid, ts);
	if (rc != RCOK)
		return rc;
	rc = ccTO.update(to_string(k1), v1 * 2, tid, ts);
	if (rc != RCOK)
		return rc;
	rc = ccTO.update(to_string(k2), v2 + 5 , tid, ts);
	if (rc != RCOK)
		return rc;
	rc = ccTO.update(to_string(k3), v3 + 10, tid, ts);
	if (rc != RCOK)
		return rc;
	return rc;
}*/

time_t get_ts() {
	latch.lock();
	time_t ts = time(0);
	Sleep(1);
	latch.unlock();
	return ts;
}

/*
RC release_lock(cc_lock& ccLock, vector<Lock> getLockList, thread::id tid) {
	RC rc = RCOK;
	for (int i = 0; i < getLockList.size(); i++) {
		rc=ccLock.lock_release(getLockList[i].type, tid, getLockList[i].key);
	}
	return rc;
}

RC transaction1(cc_lock& ccLock) {
	RC rc = RCOK;
	std::thread::id tid;
	tid = this_thread::get_id();
	vector<Lock> getLockList;
	int k1 = 0, k2 = 2;
	int v1, v2;
	rc = ccLock.get(to_string(k1), v1, tid);
	rc = ccLock.get(to_string(k2), v2, tid);
	if (v1 >= 2) {
		rc = ccLock.update(to_string(k1), v1 - 2, tid);
		Lock lock1 = Lock(to_string(k1), LOCK_EX);
		getLockList.push_back(lock1);
		rc = ccLock.update(to_string(k2), v2 + 2, tid);
		Lock lock2 = Lock(to_string(k2), LOCK_EX);
		getLockList.push_back(lock2);
	}
	rc = release_lock(ccLock, getLockList, tid);
	return rc;
}

RC transaction2(cc_lock& ccLock) {
	RC rc = RCOK;
	std::thread::id tid;
	tid = this_thread::get_id();
	vector<Lock> getLockList;
	int k1 = 2, k2 = 4;
	int v1, v2;
	rc = ccLock.get(to_string(k1), v1, tid);
	rc = ccLock.get(to_string(k2), v2, tid);
	if (v1 >= 2) {
		rc = ccLock.update(to_string(k1), v1 - 1, tid);
		Lock lock1 = Lock(to_string(k1), LOCK_EX);
		getLockList.push_back(lock1);
		rc = ccLock.update(to_string(k2), v2 + 1, tid);
		Lock lock2 = Lock(to_string(k2), LOCK_EX);
		getLockList.push_back(lock2);
	}
	rc = release_lock(ccLock, getLockList, tid);
	return rc;
}

RC transaction3(cc_lock& ccLock) {
	RC rc = RCOK;
	std::thread::id tid;
	tid = this_thread::get_id();
	vector<Lock> getLockList;
	int k1 = 1, k2 = 3, k3=5;
	int v1, v2, v3;
	rc = ccLock.get(to_string(k1), v1, tid);
	rc = ccLock.get(to_string(k2), v2, tid);
	rc = ccLock.get(to_string(k3), v3, tid);

	rc = ccLock.update(to_string(k1), v1 * 2, tid);
	Lock lock1 = Lock(to_string(k1), LOCK_EX);
	getLockList.push_back(lock1);

	rc = ccLock.update(to_string(k2), v2 + 5, tid);
	Lock lock2 = Lock(to_string(k2), LOCK_EX);
	getLockList.push_back(lock2);

	rc = ccLock.update(to_string(k3), v3 + 10, tid);
	Lock lock3 = Lock(to_string(k3), LOCK_EX);
	getLockList.push_back(lock3);
	
	rc = release_lock(ccLock, getLockList, tid);
	return rc;
}

RC transaction4(cc_lock& ccLock) {
	RC rc = RCOK;
	std::thread::id tid;
	tid = this_thread::get_id();
	vector<Lock> getLockList;

	ccLock.insert("6", 20, tid);
	datalist.push_back(6);

	
	ccLock.delete_("5", tid);
	Lock lock1 = Lock("5", LOCK_EX);
	getLockList.push_back(lock1);

	rc = release_lock(ccLock, getLockList, tid);
	return rc;
}*/