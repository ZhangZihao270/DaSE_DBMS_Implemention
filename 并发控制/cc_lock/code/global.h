#pragma once
#include<thread>

using namespace std;

/*����ֵ����������Ľ�����ͣ�RCOK ��ȷ��ERROR ����
NOT_FOUND ��������ڣ� ALREADY_EXIST �������Ѵ���
*/
enum RC { RCOK,  ERROR, FINISH, NOT_FOUND, ALREADY_EXIST };

/* ���������ͣ�EX����������SH����������NONE��������
 */
enum lock_t { LOCK_EX, LOCK_SH, LOCK_NONE };

/*�������ֲ������ԣ�LOCK ��������OCC �ֹ۲������ƣ�TO����ʱ���
*/
enum cc_type {CC_LOCK, CC_OCC, CC_TO};