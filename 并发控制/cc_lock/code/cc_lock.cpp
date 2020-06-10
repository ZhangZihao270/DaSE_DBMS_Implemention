#include <iostream>
#include <string>

#include "cc_lock.h"
#include "global.h"
#include "structure.h"

/*
 * 1. ���ж��������ϵ������Լ��ӵ����Ƿ��ͻ
 * 2. a. ������ͻ�����������ϵ�mutex��������������������������Ϣ��owner��Ϊ�Լ��ӵ���
 *    b. ����ͻ�����Լ�Ҫ�ӵ����ӵ������ݵĵȴ�����waitlist�У���������mutex������Ȼ���������ȴ����������е������ͷ�
 */
RC cc_lock::lock_get(lock_t type, thread::id tid, Data &data){
	
}

/*
 * 1. �ж������������������������󣨼��waitlist��
 * 2. a. ��û�У����ͷ�mutex���ٽ������ϵ�owner��Ϊ����
 *    b. �������������󣬴�waitlist��pop��ͷ����LockEntry����owner��Ϊ��LockEntry��Ȼ���ͷ�mutex
 */
RC cc_lock::lock_release(lock_t type, thread::id tid, Data &data){

}

/*
 * ��������̶߳��Ƕ���������û�������򲻳�ͻ�����򶼻ᷢ����ͻ
 * lock1�������ϵ�����lock2����Ҫ�ӵ���
 */
bool cc_lock::conflict_lock(LockEntry lock1, LockEntry lock2){

}

RC cc_lock::insert(string key, int value, thread::id tid){
    RC rc = RCOK;
    auto find = engine.data_map.find(key);
    if (find != engine.data_map.end() && !find->second.deleted){
        rc = ALREADY_EXIST;
    }else{
        Data data;
        data.deleted = false;
        data.value = value;
        data.owner.type = LOCK_NONE;
        engine.data_map[key] = data;
    }
    return rc;
}

/*
 * �ȼ�д������ɾ��
 */
RC cc_lock::delete_(string key, thread::id tid){
    RC rc = RCOK;
    auto find = engine.data_map.find(key);
    if (find != engine.data_map.end() && !find->second.deleted){
        rc = lock_get(LOCK_EX, tid, find->second);
        if (rc == RCOK){
            find->second.deleted = true;
        }else
            return rc;
    }else{
        rc = NOT_FOUND;
    }
    return rc;
}

/*
 * �ȼ�д�������޸�value
 */
RC cc_lock::update(string key, int value, thread::id tid){
	RC rc = RCOK;
	auto find = engine.data_map.find(key);
	if (find != engine.data_map.end() && !find->second.deleted){
		rc = lock_get(LOCK_EX, tid, find->second);
		if (rc == RCOK){
			find->second.value = value;
		}else
			return rc;
	}else{
		rc = NOT_FOUND;
	}
	return rc;
}

/*
 * �ȼӶ������ٻ�ȡvalue
 */
RC cc_lock::get(string key, int &value, thread::id tid){
	RC rc = RCOK;
	auto find = engine.data_map.find(key);

	if (find != engine.data_map.end() && !find->second.deleted){
		rc = lock_get(LOCK_SH, tid, find->second);
		value = find->second.value;
	}else{
		rc = NOT_FOUND;
	}
	return rc;
}
