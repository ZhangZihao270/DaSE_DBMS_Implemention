#pragma once
#include "global.h"
#include "structure.h"
#include<thread>
using namespace std;

class cc_lock {
public:
	cc_lock() {
		Engine engine = Engine(CC_LOCK);
	}
	cc_lock(Engine* e) {
		engine = *e;
	}

	//��ȡ��
	RC lock_get(lock_t type, thread::id tid, Data &data);

	//�ͷ���
	RC lock_release(lock_t type, thread::id tid, Data &data);

	//���²���
	RC update(string key, int value, thread::id tid);

	//ɾ������
	RC delete_(string key, thread::id tid);

	//�������
	RC insert(string key, int value, thread::id tid);

	//������
	RC get(string key, int& value, thread::id tid);

	Engine engine;
private:
	
	//�ж��������Ƿ��ͻ
	bool conflict_lock(LockEntry lock1, LockEntry lock2);
	LockEntry * get_entry();
	void return_entry(LockEntry * entry);
};

