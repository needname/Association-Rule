#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <string>
#include <iomanip>
#include <cmath>
#include <algorithm>
#include <tuple>

using namespace std;

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

long double getSupport(vector<int> item, vector<vector<int> > transactions) {
    int ret = 0;
    int k;
    for(k = 0; k < transactions.size(); ++ k){
        int i, j;
        if(transactions[k].size() < item.size()) continue;
        for(i=0, j=0; i < transactions[k].size();i++) {
            if(j==item.size()) break;
            if(transactions[k][i] == item[j]) j++;
        }
        if(j==item.size()){
            ret++;
        }
    }
    return (long double)ret/transactions.size()*100.0;

}

vector<vector<int> > generateL(vector<vector<int> > C, float minSupport, vector<vector<int> > transactions) {
    vector<vector<int> > ret;
    int i;
    for(i = 0; i < C.size(); ++i){
        long double support = getSupport(C[i], transactions);
        if(round(support, 2) < minSupport) continue;
        ret.push_back(C[i]);
    }
    return ret;
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
    for(i = 1; i < frequentSet.size(); ++i) {
       for(j = 0; j < frequentSet[i].size(); ++j) {
          generateAssociationRule(frequentSet[i][j], {}, {}, 0, associationRules, transactions);
       }
    }
    return associationRules;
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
void writeOutput(string fileName, vector<tuple<vector<int>, vector<int>, long double, long double> > associationRules) {
     ofstream fout;
     fout.open(fileName);
     if(!fout) {
         cout << "Output file could not be opened\n";
         exit(0);
     }
     int i = 0;
     for(i = 0; i < associationRules.size(); i++) {
         fout << vectorToString(get<0>(associationRules[i])) << '\t';
	 fout << "->\t";
         fout << vectorToString(get<1>(associationRules[i])) << '\t';
            
         fout << fixed;
         fout.precision(2);
         fout << "support: " << get<2>(associationRules[i]) << '\t';
            
         fout << fixed;
         fout.precision(2);
         fout << "confident: " << get<3>(associationRules[i]);
            
         fout << endl;
    }
}
    
vector<vector<int> > sortTransactions(vector<vector<int> > transactions){
    vector<vector<int> > ret;
    int i;
    for(i = 0; i < transactions.size(); ++i){
            sort(transactions[i].begin(), transactions[i].end());
            ret.push_back(transactions[i]);
    }
    return ret;
}

int main(int argc,char **argv){
    string minSup(argv[1]);
    string input(argv[2]);
    string output(argv[3]);
    vector<tuple<vector<int>, vector<int>, long double, long double> > associationRules;
    vector<vector<int> > transactions=getTransactions(input);
    float minSupport = stof(minSup);

    transactions = sortTransactions(transactions); 	
    associationRules = process(transactions,minSupport);
    writeOutput(output, associationRules);

    return 0;
}

