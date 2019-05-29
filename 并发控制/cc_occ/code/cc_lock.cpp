#include<iostream>
#include<string>
#include<thread>

#include"cc_lock.h"
#include"global.h"
#include"structure.h"

/*
RC cc_lock::lock_get(lock_t type, thread::id tid, string key)
{
	RC rc=RCOK;
	//lock_t type1 = LOCK_NONE;
	LockEntry lock1;
	LockEntry lock;

	lock.type = type;
	lock.tid = tid;

	auto find = engine.data_map.find(key);
	if (find != engine.data_map.end() && !find->second.deleted) {
		lock1 = find->second.owner;
	}
	else {
		rc = NOT_FOUND;
	}
	bool conflict = conflict_lock(lock1, lock);
	if (conflict) {
		find->second.waitlist.push(&lock);
		find->second.latch.lock();
		find->second.owner = lock;
		find->second.waitlist.pop();
	}
	else{
		if (lock1.tid == lock.tid&&lock.type==LOCK_EX) 
			find->second.owner = lock;
		else if (lock1.tid != lock.tid) {
			find->second.latch.lock();
			find->second.owner = lock;
		}
	}
	return rc;
}

RC cc_lock::lock_release(lock_t type, thread::id tid, string key)
{
	RC rc = RCOK;
	Data * data;
	auto find = engine.data_map.find(key);
	if (find != engine.data_map.end() && !find->second.deleted) {
		data = &find->second;
	}
	else {
		rc = NOT_FOUND;
		return rc;
	}

	if (data->waitlist.size() == 0) {
		data->owner.type=LOCK_NONE;
		data->latch.unlock();
	}
	else {
		LockEntry * firstWaitLock = data->waitlist.front();
		data->owner = *firstWaitLock;
		data->latch.unlock();
	}
	return rc;
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

bool cc_lock::conflict_lock(LockEntry lock1, LockEntry lock2)
{
	if (lock1.tid == lock2.tid)
		return false;
	else if (lock1.type != LOCK_NONE || lock2.type != LOCK_NONE)
		return true;
	else
		return false;
}
*/