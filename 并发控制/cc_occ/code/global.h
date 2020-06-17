#pragma once
#include<iostream>
#include<thread>

using namespace std;

/*����ֵ����������Ľ�����ͣ�RCOK ��ȷ��ERROR ����
NOT_FOUND ��������ڣ� ALREADY_EXIST �������Ѵ���
*/
enum RC { RCOK,  RCERROR, ABORT, WAIT, FINISH, NOT_FOUND, ALREADY_EXIST };

/* ���������ͣ�EX����������SH����������NONE��������
 */
enum lock_t { LOCK_EX, LOCK_SH, LOCK_NONE };

/*�������ֲ������ԣ�LOCK ��������OCC �ֹ۲������ƣ�TO����ʱ���
*/
enum cc_type {CC_LOCK, CC_OCC, CC_TO};

/*
request type, R_REQ: read request, W_REQ: write request, P_REQ: pre-write request
*/
enum TsType { R_REQ, W_REQ, P_REQ, XP_REQ };

/* general concurrency control */
enum access_t { RD, WR, XP, SCAN };