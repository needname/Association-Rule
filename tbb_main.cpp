#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <string>
#include <iomanip>
#include <cmath>
#include <algorithm>
#include <tuple>
#include "tbb/parallel_for.h"
#include "tbb/blocked_range2d.h"
#include "tbb/blocked_range.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/parallel_reduce.h"
#include "tbb/parallel_while.h"
#include "tbb/parallel_sort.h"

using namespace std;
using tbb::parallel_for;
using tbb::blocked_range;
using tbb::blocked_range2d;
using tbb::split;
using tbb::parallel_reduce;

long double round(long double value, int pos){
    long double temp;
    temp = value * pow( 10, pos );
    temp = floor( temp + 0.5 );
    temp *= pow( 10, -pos );
    return temp;
}

string vectorToString(vector<int> arr) {
    string ret = "{";
    for(int i=0;i<arr.size();i++){
        ret += to_string(arr[i]);
        if(i != arr.size()-1){
            ret += ",";
        }
    }
    ret += "}";
    return ret;
}

vector<vector<int> > generateL(vector<vector<int> > C, float minSupport, vector<vector<int> > transactions);

//Tao tap C : tham so gom : buoc di hien tai va tap transactions(lan dau) , tap C truoc do (lan thu 2 tro di)
//

vector<vector<int> > generateNextC(int nowStep,vector<vector<int> > transactions){
    vector<vector<int> > C;
    int i,j,k;
    if (nowStep==1) { //Lan generate dau sinh ra C co dang la C
        set<int> element;
        for (i=0; i<transactions.size(); i++) {
            for (j=0; j<transactions[i].size(); j++) {
                element.insert(transactions[i][j]);
            }
        }
        
        for(auto iter=element.begin(); iter != element.end(); iter++){
            C.push_back(vector<int>(1, *iter));
        }
    }
    else{
        set<int> Stempnext;
        bool check;
        for (i=0; i<transactions.size(); i++) {
            set<int> Stemp;
            for (j=0; j<transactions[i].size(); j++) {
                Stemp.insert(transactions[i][j]);
            }
            for (j=i+1; j<transactions.size(); j++) {
                Stempnext=Stemp;
                Stempnext.insert(transactions[j].begin(),transactions[j].end());
                if (Stempnext.size()==nowStep) {
                    check=false;
                    vector<int> Vtemp;
                    for(auto iter=Stempnext.begin(); iter != Stempnext.end(); iter++){
                        Vtemp.push_back(*iter);
                    }
                    for (k=0 ; k < C.size(); k++) {
                        if (Vtemp==C[k]) {
                            check=true;
                            break;
                        }
                    }
                    if (check==false) {
                        C.push_back(Vtemp);
                    }
                }
            }
        }
    }
    return C;
}

class Support{
	vector<int> _item;
	vector<vector<int> > _transactions;
public:
	int ret;
	void operator() ( const blocked_range<size_t>& r){
		for( size_t k=r.begin(); k!=r.end(); ++k){
			int i, j;	
		        if(_transactions[k].size() < _item.size()) continue;
		        for(i=0, j=0; i < _transactions[k].size();i++) {
		            if(j==_item.size()) break;
		            if(_transactions[k][i] == _item[j]) j++;
        		}
		        if(j==_item.size()){
		            ret++;
			}
		}
	}
	
	Support( Support& x, split ): _item(x._item), _transactions(x._transactions), ret(0) {}
	void join( const Support& y ) {ret += y.ret;}
	Support(vector<int> item, vector<vector<int> > transactions): _item(item), _transactions(transactions), ret(0) {}
};

long double getSupport(vector<int> item, vector<vector<int> > transactions) {
    int ret = 0;
    Support s(item, transactions);
    parallel_reduce(blocked_range<size_t>(0,transactions.size()), s, tbb::auto_partitioner());
    ret = s.ret;
    return (long double)ret/transactions.size()*100.0;
}

class L{
	vector<vector<int> > _C;
	float _minSupport;
	vector<vector<int> > _transactions;
public:
	vector<vector<int> > ret;
	void operator()( const blocked_range<size_t>& r ){
		for(size_t i = r.begin(); i != r.end(); ++i){	
	        	long double support = getSupport(_C[i], _transactions);
        		if(round(support, 2) < _minSupport) continue;
		        ret.push_back(_C[i]);
		}
	}

	L( L& x, split ): _C(x._C), _minSupport(x._minSupport), _transactions(x._transactions), ret() {}
	void join( const L& y ) {ret.insert(ret.end(), (y.ret).begin(), (y.ret).end());}

	L( vector<vector<int> > C, float minSupport, vector<vector<int> > transactions) : _C(C), _minSupport(minSupport), _transactions(transactions), ret() {}
};

vector<vector<int> > generateL(vector<vector<int> > C, float minSupport, vector<vector<int> > transactions) {
    L l(C, minSupport, transactions);
    parallel_reduce(blocked_range<size_t>(0,C.size()),l,tbb::auto_partitioner());
    return l.ret;
}

void generateAssociationRule(vector<int> items, vector<int> X, vector<int> Y, int index, vector<tuple<vector<int>, vector<int>, long double, long double> > &associationRules, vector<vector<int> > transactions) {
    if(index == items.size()) {
        if(X.size()==0 || Y.size() == 0) return;
        long double XYsupport = getSupport(items, transactions);
        long double Xsupport = getSupport(X, transactions);
            
        if(Xsupport == 0) return;
            
        long double support = (long double)XYsupport;
        long double confidence = (long double)XYsupport/Xsupport*100.0;
        associationRules.push_back(tuple<vector<int>, vector<int>, long double, long double>(X, Y, support, confidence));
        return;
    }
     
    X.push_back(items[index]);
    generateAssociationRule(items, X, Y, index+1,associationRules,transactions);
    X.pop_back();
    Y.push_back(items[index]);
    generateAssociationRule(items, X, Y, index+1,associationRules,transactions);
}

class AssociationRule{
	vector<vector<int> > _transactions;
    	vector<vector<vector<int> > > _frequentSet;
public:
	vector<tuple<vector<int>, vector<int>, long double, long double> > associationRules;
	void operator()( const blocked_range<size_t>& r ){
		for(size_t i = r.begin(); i != r.end(); ++i){
			for(int j = 0; j < _frequentSet[i].size(); ++j)
				generateAssociationRule(_frequentSet[i][j], {}, {}, 0, associationRules, _transactions);
		}
	}
	AssociationRule( AssociationRule& x, split ) : associationRules(), _frequentSet(x._frequentSet), _transactions(x._transactions){}
	
	void join( const AssociationRule& y ) {
		associationRules.insert(associationRules.end(),(y.associationRules).begin(), (y.associationRules).end());
	}

	AssociationRule(vector<vector<vector<int> > > frequentSet, vector<tuple<vector<int>, vector<int>, long double, long double> > associationRules, vector<vector<int> > transactions): _frequentSet(frequentSet), associationRules(associationRules), _transactions(transactions) {} 
};

vector<tuple<vector<int>, vector<int>, long double, long double> > process( vector<vector<int> > transactions, float minSupport) {
    vector<tuple<vector<int>, vector<int>, long double, long double> > associationRules;    
    vector<vector<int> > C, L;
    vector<vector<vector<int> > > frequentSet;
    int nowStep = 1;

    int i = 0, j = 0, k = 0;
    while(true) {
        if (nowStep==1) {
            C = generateNextC(nowStep, transactions);
        }
        else{
            C = generateNextC(nowStep, L);
        }
        if(C.size()==0) break;
        nowStep++;
	L = generateL(C, minSupport, transactions);
        frequentSet.push_back(L);
    }
    AssociationRule a(frequentSet, associationRules, transactions);
    parallel_reduce(blocked_range<size_t>(1, frequentSet.size()), a, tbb::auto_partitioner()); 
    return a.associationRules;
}

/*Read input*/
vector<vector<int> > getTransactions(string input){
    ifstream f;
    f.open(input);
    vector<vector<int> > transactions;
    vector<int> temp;
    string str;
    while(!getline(f, str).eof()){
        vector<int> temp;
        int pre = 0;
        for(int i=0;i<str.size()-1;i++){
            if(str[i] == ' ') {
                int num = atoi(str.substr(pre, i).c_str());
                temp.push_back(num);
                pre = i+1;
            }
        }
        int num = atoi(str.substr(pre, str.size()).c_str());
        temp.push_back(num);
        transactions.push_back(temp);
    }
    return transactions;
}

/*Write output*/

class Output {
	vector<tuple<vector<int>, vector<int>, long double, long double> > _associationRules;
public:
	string temp;
	void operator()( const blocked_range<size_t>& r ){
		for( size_t i=r.begin(); i != r.end(); ++i ){
			temp += vectorToString(get<0>(_associationRules[i]));
			temp += "\t";
			temp += vectorToString(get<1>(_associationRules[i]));
			temp += "\t";
			temp += to_string(get<2>(_associationRules[i]));
			temp += "\t";
			temp += to_string(get<3>(_associationRules[i]));
			temp += "\n";
		}
	}
	Output( Output& x, split ) : _associationRules(x._associationRules), temp("") {}
	void join( const Output& y ) {temp+=y.temp;}
	Output(	vector<tuple<vector<int>, vector<int>, long double, long double> > associationRules ) : _associationRules(associationRules), temp("") {}
};

void writeOutput(string fileName, vector<tuple<vector<int>, vector<int>, long double, long double> > associationRules) {
     ofstream fout;
     fout.open(fileName);
     if(!fout) {
         cout << "Output file could not be opened\n";
         exit(0);
     }
     int i = 0;	
     Output out(associationRules);
     parallel_reduce(blocked_range<size_t>(0,associationRules.size()),out,tbb::auto_partitioner());
     fout << out.temp;	
}

class SortT{
	vector<vector<int> > _transactions;
public:
	vector<vector<int> > ret;
	void operator()( const blocked_range<size_t>& r ) {
		for( size_t i = r.begin(); i != r.end(); ++i ){
			tbb::parallel_sort(_transactions[i].begin(), _transactions[i].end());
			ret.push_back(_transactions[i]);
		}
	}
	SortT( SortT& x, split ) : _transactions(x._transactions), ret() {}
	
	void join( const SortT& y) { ret.insert(ret.end(),(y.ret).begin(), (y.ret).end()); 
	}
	
	SortT( vector<vector<int> > transactions ) : _transactions( transactions ), ret() {}
};
    
vector<vector<int> > sortTransactions(vector<vector<int> > transactions){
    SortT s(transactions);
    parallel_reduce(blocked_range<size_t>(0,transactions.size()),s, tbb::auto_partitioner());	
    return s.ret;
}

int main(int argc,char **argv){
    string minSup(argv[1]);
    string input(argv[2]);
    string output(argv[3]);
    vector<tuple<vector<int>, vector<int>, long double, long double> > associationRules;    
    vector<vector<int> > transactions=getTransactions(input);

    int threads = (argc > 3) ? atoi(argv[4]) : tbb::task_scheduler_init::default_num_threads();
    tbb::task_scheduler_init init( threads );
    float minSupport = stof(minSup);

    transactions = sortTransactions(transactions); 	
    associationRules = process(transactions,minSupport);
    writeOutput(output, associationRules);
    return 0;
}

