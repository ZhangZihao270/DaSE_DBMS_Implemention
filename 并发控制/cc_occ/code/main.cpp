#include<iostream>
#include<string>
#include<queue>
#include<vector>
#include<thread>
#include<ctime>
#include<Windows.h>
#include"cc_occ.h"

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
	}
	return 0;
}

void initial_data(Engine& engine, vector<int> &datalist) {
	for (int i = 0; i < 6; i++) {
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

//更新数据0和2
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
	rc = ccOCC.commit(txn);
	cout <<"txn1:"<< txn->tid << "  s:  " << txn->start_ts << "  e:  " << txn->end_ts << "  c:  " << txn->commit_ts << "  his_len: " << ccOCC.occ_man.his_len << endl;
	return rc;
}

//更新数据0和4
RC transaction2(cc_occ& ccOCC){
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
	rc = ccOCC.commit(txn);
	cout <<"txn2:"<< txn->tid << "  s:  " << txn->start_ts << "  e:  " << txn->end_ts << "  c:  " << txn->commit_ts << "  his_len: " << ccOCC.occ_man.his_len << endl;
	return rc;
}

//更新数据1，3，5
RC transaction3(cc_occ& ccOCC){
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
	rc = ccOCC.commit(txn);
	cout <<"txn3:"<< txn->tid << "  s:  " << txn->start_ts << "  e:  " << txn->end_ts << "  c:  " << txn->commit_ts <<"  his_len: "<<ccOCC.occ_man.his_len<< endl;
	return rc;
}

time_t get_ts() {
	latch.lock();
	time_t ts = time(0);
	Sleep(100);
	latch.unlock();
	return ts;
}