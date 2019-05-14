#include<iostream>
#include<string>
#include<thread>

#include"cc_lock.h"
#include"global.h"
#include"structure.h"

RC cc_lock::lock_get(lock_t type, thread::id tid, string key)
{
	RC rc=RCOK;

	/*请同学们完善加锁流程*/

	return rc;
}

RC cc_lock::lock_release(lock_t type, thread::id tid, string key)
{
	RC rc = RCOK;
	/*请同学们完善解锁流程*/
	return rc;
}

bool cc_lock::conflict_lock(LockEntry lock1, LockEntry lock2)
{
	/*请同学们完善冲突的判断条件*/
}


RC cc_lock::update(string key, int value, thread::id tid)
{
	RC rc = RCOK;
	auto find = engine.data_map.find(key);
	if (find != engine.data_map.end() && !find->second.deleted) {
		rc = lock_get(LOCK_EX, tid, key);
		if (rc == RCOK) {
			find->second.value = value;
		}
		else
			return rc;

		//rc = lock_release(LOCK_EX, tid, key);
	}
	else {
		rc = NOT_FOUND;
	}
	return rc;
}

RC cc_lock::delete_(string key, thread::id tid)
{
	RC rc = RCOK;
	auto find = engine.data_map.find(key);
	if (find != engine.data_map.end() && !find->second.deleted) {
		rc = lock_get(LOCK_EX, tid, key);
		if (rc == RCOK) {
			find->second.deleted = true;
		}
		else
			return rc;

		//rc = lock_release(LOCK_EX, tid, key);
	}
	else {
		rc = NOT_FOUND;
	}
	return rc;
}

RC cc_lock::insert(string key, int value, thread::id tid)
{
	RC rc = RCOK;
	auto find = engine.data_map.find(key);
	if (find != engine.data_map.end() && !find->second.deleted) {
		rc = ALREADY_EXIST;
	}
	else {
		Data data;
		data.deleted = false;
		data.value = value;
		data.owner.type=LOCK_NONE;
		//data->value = value;
		engine.data_map[key] = data;
	}
	return rc;
}

RC cc_lock::get(string key, int& value, thread::id tid)
{
	RC rc = RCOK;
	auto find = engine.data_map.find(key);
	
	if (find != engine.data_map.end() && !find->second.deleted) {
		rc = lock_get(LOCK_SH, tid, key);
		value = find->second.value;
	}
	else {
		rc = NOT_FOUND;
	}
	return rc;
}

