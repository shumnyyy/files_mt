#include <iostream>
#include <thread>
#include <mutex>
#include <fstream>
#include <cstdlib>
#include <vector>
#include <ctime>
#include <sstream>

#include <iterator>
#include <algorithm>
#include <numeric>
#include <functional>
#include <queue>
#include <condition_variable>
#include <time.h>
#include <stdexcept>
#include <string>
#include <cassert> 

using namespace std;

vector<string> file_names; //вектор из названий файликов, которые будут создаваться
std::mutex m2, m3, m4;
//Библиотека потоков гарантирует,
//что, как только один поток заблокирует определенный мьютекс,
//все остальные потоки, пытающиеся его заблокировать,
//должны будут ждать, пока поток, который успешно заблокировал мьютекс, его не разблокирует.
//Тем самым гарантируется, что все потоки видят непротиворечивое представление совместно используемых данных

const int SIZE = 100;

void print_file(string ans) {
	m4.lock();
	cout << "FILE NAME: " << ans << endl;
	ans = "bin1/" + ans;
	FILE* f5 = fopen(ans.c_str(), "rb");
	assert(f5);
	int y;
	size_t r5 = fread(&y, sizeof(int), 1, f5);
	int cnt = 1;
	while (r5 > 0) {
		cout << y << ' ';
		r5 = fread(&y, sizeof(int), 1, f5);
		if (cnt++ % 20 == 0)
			cout << endl;
	}
	fclose(f5);
	m4.unlock();
}

void read_file(mutex& m, FILE* f) {
	vector<int> buff(SIZE);
	int cnt = 0;
	while (true) {
		m.lock();
		size_t M = fread(&buff[0], sizeof(buff[0]), SIZE, f);
		m.unlock();

		if (M == 0) {
			break;
		}

		if (M < SIZE) {
			buff.resize(M);
			//break;
		}

		sort(buff.begin(), buff.end());

		m2.lock();
		string new_file_name;
		stringstream ss, ss2;
		ss << this_thread::get_id();
		ss2 << cnt;
		new_file_name = "bin1/" + ss.str() + "_" + ss2.str();
		++cnt;

		file_names.push_back(ss.str() + "_" + ss2.str());
		cout << "FILE: " << new_file_name << endl;

		FILE* f1 = fopen(new_file_name.c_str(), "wb");
		assert(f1);
		fwrite(&buff[0], sizeof(buff[0]), M, f1);
		fclose(f1);
		m2.unlock();

		cout << "New file: " << new_file_name << endl;

		//print_file(ss.str() + "_" + ss2.str());
	}
}

//cлияние файлов, на которые разбивается бинарник
void merge2files(string a, string b, string combo_name) {
	string name1 = "bin1/" + a;
	string name2 = "bin1/" + b;

	int x1 = 0, x2 = 0;

	//cout << "name1: " << name1 << endl;
	FILE* f1 = fopen(name1.c_str(), "rb");
	assert(f1);
	FILE* f2 = fopen(name2.c_str(), "rb");
	assert(f2);

	m3.lock();
	size_t M1 = fread(&x1, sizeof(int), 1, f1);
	size_t M2 = fread(&x2, sizeof(int), 1, f2);
	m3.unlock();

	string new_name = "bin1/" + combo_name;
	FILE* file = fopen(new_name.c_str(), "wb");
	assert(file);

	while (M1 > 0 && M2 > 0) {
		if (x1 < x2) {
			m3.lock();
			fwrite(&x1, sizeof(int), 1, file);
			M1 = fread(&x1, sizeof(int), 1, f1);
			m3.unlock();
			//continue;
		}
		else {
			m3.lock();
			fwrite(&x2, sizeof(int), 1, file);
			M2 = fread(&x2, sizeof(int), 1, f2);
			m3.unlock();
			//continue;
		}
	}

	//проверки на непустоту
	while (M1 > 0) {
		m3.lock();
		fwrite(&x1, sizeof(int), 1, file);
		M1 = fread(&x1, sizeof(int), 1, f1);
		m3.unlock();
	}

	while (M2 > 0) {
		m3.lock();
		fwrite(&x2, sizeof(int), 1, file);
		M2 = fread(&x2, sizeof(int), 1, f2);
		m3.unlock();
	}

	fclose(f1);
	fclose(f2);
	fclose(file);

	remove(name1.c_str());
	remove(name2.c_str());

}

//очередь без изменений
class SafeQueue {
	queue<string> q;
	condition_variable c;
	mutable mutex m;

	mutable mutex vector_m;
	vector<bool> everybody_works;

public:
	SafeQueue(int thread_num) {
		everybody_works.assign(thread_num, false);
	}

	SafeQueue(SafeQueue const& other) {
		lock_guard<mutex> g(other.m);
		lock_guard<mutex> gv(other.vector_m);
		q = other.q;
		everybody_works = other.everybody_works;
	}

	SafeQueue(queue<string> const& default_queue, int thread_num)
	{
		q = default_queue;
		everybody_works.assign(thread_num, false);
	}

	void push(string val)
	{
		lock_guard<mutex> g(m);
		q.push(val);
		c.notify_one();
	}

	int size() {
		lock_guard<mutex> g(m);
		return q.size();
	}

	void set_me_working(int th, bool val) {
		lock_guard<mutex> g(vector_m);
		everybody_works[th] = val;
		c.notify_one();
	}

	bool is_everybody_working() {
		lock_guard<mutex> g(vector_m);
		return accumulate(everybody_works.begin(), everybody_works.end(), false);
	}

	string just_pop()
	{
		lock_guard<mutex> lk(m);
		if (q.empty())
			throw "No elems";
		string a = q.front();
		q.pop();
		return a;
	}

	bool wait_pop(string& a, string& b)
	{
		unique_lock<mutex> lk(m);
		c.wait(lk, [this] {return q.size() > 1 || !is_everybody_working(); });
		if (q.empty()) {
			throw "Error!";
		}
		if (q.size() == 1)
			return false;
		a = q.front();
		q.pop();
		b = q.front();
		q.pop();
		return true;
	}

};

void thread_work(SafeQueue& q, int my_num) //мержим файлы
{
	int cnt = 0;
	while (true) {
		string a, b;
		bool go = q.wait_pop(a, b);

		if (!go)
			break;

		q.set_me_working(my_num, true);

		//имя нового файла
		string m;
		stringstream ss, ss2;
		ss << this_thread::get_id();
		ss2 << cnt;
		m = ss.str() + "_0_" + ss2.str();
		//cout << "M: " << m << endl;
		++cnt;

		merge2files(a, b, m);

		this_thread::sleep_for(chrono::milliseconds(100));

		//print_file(m);

		q.push(m);
		q.set_me_working(my_num, false);
	}

	q.set_me_working(my_num, false);
}

string find_max_multi_thread(vector<string> v, int req_num_treads)
{
	queue<string> default_queue;
	for (auto& a : v)
		default_queue.push(a);

	int max_threads = thread::hardware_concurrency();
	int num_threads = min(max_threads, req_num_treads);
	vector<thread> threads(num_threads - 1);

	SafeQueue q(default_queue, num_threads);

	try {
		int tres, tstart, tend;
		auto start = std::chrono::steady_clock::now();

		for (int i = 0; i < num_threads - 1; i++)
			threads[i] = thread(thread_work, ref(q), i);
		thread_work(ref(q), num_threads - 1);

		for_each(threads.begin(), threads.end(), mem_fn(&thread::join));

		auto end = std::chrono::steady_clock::now();
		auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
			end - start);

		cout << "Done by " << num_threads << " thread(s) in " << elapsed.count()
			<< " milisecs." << endl;

		return q.just_pop();

	}
	catch (...) {
		for_each(threads.begin(), threads.end(), mem_fn(&thread::join));
		throw;
	}

}

int main(int argc, char** argv) {
	srand(time(NULL));

	//генерим файлы
	int N = 10000;
	int a[10000];
	for (int i = 0; i < N; ++i) {
		//a[i] = 30 - i;
		a[i] = rand() % 100;
	}

	FILE *f1 = fopen("file.txt", "wb");
	assert(f1);
	size_t r1 = fwrite(a, sizeof a[0], N, f1);
	fclose(f1);
	

	FILE *f2 = fopen("file.txt", "rb");
	//FILE* f2 = fopen(argv[1], "rb");

	std::mutex m;

	std::cout << "starting first helper...\n";
	std::thread t1(read_file, ref(m), f2);

	std::cout << "starting second helper...\n";
	std::thread t2(read_file, ref(m), f2);

	std::cout << "waiting for helpers to finish..." << std::endl;
	t1.join();
	t2.join();
	/*
	 t3.join();
	 t4.join();
	 */
	std::cout << "done!\n";
	fclose(f2);

	cout << "starting merging...\n";

	string ans;

	try {

		string m;
		m = find_max_multi_thread(file_names, 100); // comp uses 4 threads max
		cout << "Final file name: " << m << endl;
		ans = m;

	}
	catch (...) {
		cout << "Ooops!" << endl;
		return 0;
	}

	cout << "Done!" << endl;

	//print_file(ans);
	return 0;
}