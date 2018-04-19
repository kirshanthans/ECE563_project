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
#define REPEAT_INPUT 5 // repeat the same set of input this many times
#define min(a, b) a<b? a : b
#define MIN(pid, arraySize, numP) (int)(pid*(arraySize/(double)numP))
#define MAX(pid, arraySize, numP) (int)((pid+1)*(arraySize/(double)numP)) - 1


extern "C" struct hashMap** run_omp(int fnum, char** files, int nThreads, int pid, int numproc);

void update_map(string word, int count, std::map<string,int>& wc_map){
    std::map<string, int>::iterator it = wc_map.find(word);
    if(it != wc_map.end()) it->second += count;
    else{
        wc_map.insert(std::pair<string, int>(word, count));
    }
    
}

void run_receiver(int numP, int pid, std::map<string, int>& wc_map){
    // first read files relavent to this pid and update the wc_map
    char fname[64];
    sprintf(fname, "Output_pid%d_tid%d.txt", pid, pid);
    std::ifstream infile(fname);
    string line;
    while(std::getline(infile, line)){
        char * cline = (char*)line.c_str();
        string word = string(strtok(cline, " "));
        char * count = strtok(NULL, " ");
        if(!count) continue; // TODO : this is a bug in omp version
        else{
            update_map(word, atoi(count), wc_map);
        }
    }
    // remove the local file
    if(remove(fname) != 0) cout << "Erorr deleting file : " << fname << endl;
    //here the receiver is reading from all processes in a worklist mannar
    //i.e. using ANY_SOURCE since we don't know the order msgs are received
    MPI_Status status;
    std::map<int, bool> done_map; // this maps keeps track of which processes are done
    for(int i=1; i<numP; i++) done_map.insert(std::pair<int, bool>(i, false));
    bool done = false;
    while(!done){
        done = true;
        for(int i=1; i<numP; i++){
            if(i==pid || done_map[i] ) continue;
            char msg[512];
            MPI_Recv(msg, 512, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            
            string msgstr(msg);
            // first check end of file is received. if so we no longer in that
            // source
            if(msgstr.compare("end of file") == 0){
                done_map[status.MPI_SOURCE] = true;
                
            }
            else{
                string word = string(strtok((char*)msgstr.c_str(), " "));
                char * count = strtok(NULL, " ");
                if(count){
                    //cout << "word : " << word << "count :  " << count << endl;
                    update_map(word, atoi(count), wc_map);
                    // we found an entry so repeat loop one more time
                    done = false;
                }
            }
        }
     }


}

void run_sender(int numP, int pid){

    for(int i=1; i<numP; i++){
        if(i==pid) continue;

        // read the file and send data to relavent process
        char fname[64];
        sprintf(fname, "Output_pid%d_tid%d.txt", pid, i);
        std::ifstream infile(fname);
        string line;
        while(std::getline(infile, line)){
            char copy[512];
            strcpy(copy, line.c_str());
            copy[511] = '\0';
            MPI_Send(copy, 512, MPI_CHAR, i, i, MPI_COMM_WORLD); 
        }
        
        // here we send a end of file string
        string endoffile = "end of file";
        char eofstr[512];
        strcpy(eofstr, endoffile.c_str());
        MPI_Send(eofstr, 512, MPI_CHAR, i, i, MPI_COMM_WORLD);
   
        //remove the file
        if(remove(fname) != 0) cout << "Error deleting file : " << fname << endl;
    }
}




int main(int argc, char* argv[]){
    int pid, numproc;
    MPI_Status status;
    int provided;
    MPI_Init_thread(&argc, &argv, NTHREADS, &provided); // not sure about this?
    
    //MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numproc);   
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);  
    
    // timing
    double elapsed = -MPI_Wtime();

    // every node will ask files from master. If files available
    // master will send '1' follwed by the file name. else send '0' (all done).
    if(pid == 0){
        DIR* dir;
        struct dirent * entry;
        vector<string> filenames;
        string input_dir = "../RawText/";

        if((dir = opendir(input_dir.c_str())) != NULL){
            while((entry = readdir(dir)) != NULL){
                string fname(entry->d_name);
                //cout << fname << endl;
                if(fname == "." || fname == "..") continue;
                
                filenames.push_back(input_dir+fname);
            }
            closedir(dir);
        }
        else{
            // error
            return -1;
        }
        
        // Now to increse the size of input reuse the same input files 
        unsigned int no_of_files = filenames.size();
        //cout << no_of_files << endl;
        for(int i=0; i<REPEAT_INPUT-1; i++){
            for(unsigned int j=0; j<no_of_files; j++) filenames.push_back(filenames[j]);
        }

        cout << "No of input files for parallel word count = " << filenames.size() << endl;
        //send FNUM files to each process
        while(!filenames.empty()){
            for(int i=1; i<numproc; i++){
                
                int no_files_to_sent = min(FNUM, filenames.size());

                for(int j=0; j<no_files_to_sent; j++){
                    // TODO : handle overflows
                    char fname[FLEN];
                    strcpy(fname, filenames[j].c_str());
                    
                    // do a MPI_Send
                    MPI_Send(fname, FLEN, MPI_CHAR, i, i, MPI_COMM_WORLD);
                    
                }
                // remove the sent files
                filenames.erase(filenames.begin(), filenames.begin()+no_files_to_sent);

                if(filenames.empty()) break; 
            }
        }

        // sedn "all done!" msg to all processes
        string s = "all done!";
        char all_done_msg[FLEN];
        strcpy(all_done_msg, s.c_str());
        for(int i=1; i<numproc; i++){
            MPI_Send(all_done_msg, FLEN, MPI_CHAR, i, i, MPI_COMM_WORLD);
        }

    }
    else{
        
        bool all_done = false;
        string s = "all done!";
        char fname[FLEN];
        vector<string> fnames;
        // poll for filenames
        while(!all_done){
            MPI_Recv(fname, FLEN, MPI_CHAR, 0, pid, MPI_COMM_WORLD, &status);
            
            if(strcmp(s.c_str(), fname) == 0) all_done = true;
            else{
            
                string fstr(fname);
                fnames.push_back(fstr);
            
            }
        }

        // call the OMP routine for mapping
        char** filenames = (char**)malloc(sizeof(char*)*fnames.size());
        
        for(unsigned int i=0; i<fnames.size(); i++){
            
            filenames[i] = (char*)fnames[i].c_str();
        
        }
        
        struct hashMap**  maps = run_omp((int)fnames.size(), filenames, NTHREADS, pid, numproc);
        
       
        // now combine the results globally communicating with other processes
        // we will use sender and reciever threads
        std::map<string, int> word_count_map;
        #pragma omp parallel sections 
        {
            #pragma omp section
            {
                run_sender(numproc, pid);
            }
            #pragma omp section
            {
                run_receiver(numproc, pid, word_count_map);
            }
        }
        // here write the output to a file
        //cout << "pid : " << pid << " size of word map : " << word_count_map.size() << endl;
        char outfname[64];
        sprintf(outfname, "Result_pid%d.txt", pid);
        FILE* pFile = fopen(outfname, "w");


        std::map<string, int>::iterator it = word_count_map.begin();
        for(; it!=word_count_map.end(); it++){
            string wordstr = it->first;
            fprintf(pFile, "%-128s%d\n", wordstr.c_str(), it->second);
            //outfile << it->first << string(" ", 128) << it->second << endl;
            //cout << it->first << endl;
            //cout << it->second << endl;
        }
        fclose(pFile);

        free(filenames);
    
    }

    // barrier for timing
    MPI_Barrier(MPI_COMM_WORLD);
    elapsed += MPI_Wtime();
    if(pid == 0) cout << "Elapsed time for parallel implementation = " << elapsed << endl;

    MPI_Finalize();
    return 0;
}
