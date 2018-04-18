#include <dirent.h>
#include <map>
#include <mpi.h>
#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include "omp_map_reduce.h"
using namespace std;
#define REPEAT_INPUT 1 // repeat the same set of input this many times
#define min(a, b) a<b? a : b
#define MIN(pid, arraySize, numP) (int)(pid*(arraySize/(double)numP))
#define MAX(pid, arraySize, numP) (int)((pid+1)*(arraySize/(double)numP)) - 1


extern "C" struct hashMap** run_omp_seq(int fnum, char** files);

//void update_map(string word, int count, std::map<string,int>& wc_map){
    //std::map<string, int>::iterator it = wc_map.find(word);
    //if(it != wc_map.end()) it->second += count;
    //else{
        //wc_map.insert(std::pair<string, int>(word, count));
    //}
    
//}



int main(int argc, char* argv[]){
    DIR * dir;
    struct dirent * entry;
    vector<string> fnames;
    string input_dir = "../RawText/";

    if((dir = opendir(input_dir.c_str())) != NULL){
        while((entry = readdir(dir)) != NULL){
            string fname(entry->d_name);
            //cout << fname << endl;
            if(fname == "." || fname == "..") continue;
            
            fnames.push_back(input_dir+fname);
        }
        closedir(dir);
    }
    else{
        // error
        return -1;
    }
    
    // Now to increse the size of input reuse the same input files 
    unsigned int no_of_files = fnames.size();
    //cout << no_of_files << endl;
    for(int i=0; i<REPEAT_INPUT-1; i++){
        for(unsigned int j=0; j<no_of_files; j++) fnames.push_back(fnames[j]);
    }

        
    // call the OMP routine for mapping
    char** filenames = (char**)malloc(sizeof(char*)*fnames.size());
    
    for(unsigned int i=0; i<fnames.size(); i++){
        
        filenames[i] = (char*)fnames[i].c_str();
    
    }
    
    // run the sequential version of map-reduce
    run_omp_seq((int)fnames.size(), filenames);

    
    free(filenames);
    
    return 0;
}
