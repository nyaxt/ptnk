#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include "bench.h"

const char* PROGNAME = "benchprog";
int NUM_KEYS;
int NUM_TX = 10000;
int NUM_W_PER_TX = 100;
int NUM_R_PER_TX = 0;
int NUM_THREADS = 1;
int NUM_PREW = 0;
const char* dbfile = NULL;
bool do_sync = false, do_intensiverebase = false, do_random = false, do_load = false;
const char* comment = "";

void run_bench(void);

void help()
{
	printf("%s opts dbfile\n", PROGNAME);
	exit(1);
}

int* keys;
int written_keys = 0;

struct option g_opts[] = {
	{"sync", 0, NULL, 0},
	{"comment", 1, NULL, 0},
	{"numtx", 1, NULL, 0},
	{"numR", 1, NULL, 0},
	{"numW", 1, NULL, 0},
	{"intensiverebase", 0, NULL, 0},
	{"random", 0, NULL, 0},
	{"checkdb", 0, NULL, 0},
	{"numthr", 1, NULL, 0},
	{"sleep", 0, NULL, 0},
	{"numPreW", 1, NULL, 0},
	{0, 0, NULL, 0}
};

int main(int argc, char** argv)
{
	PROGNAME = argv[0];

	int c, oi;
	while((c = ::getopt_long(argc, argv, "", g_opts, &oi)) != -1) switch(c) {
	case 0: /* long opt */
		switch(oi)
		{
		case 0:
			do_sync = true;
			break;

		case 1:
			comment = optarg;
			break;

		case 2:
			NUM_TX = atoi(optarg);
			break;

		case 3:
			NUM_R_PER_TX = atoi(optarg);
			break;

		case 4: 
			NUM_W_PER_TX = atoi(optarg);
			break;

		case 5: 
			do_intensiverebase = true;
			break;

		case 6:
			do_random = true;
			break;

		case 7: 
			do_load = true;
			break;

		case 8:
			NUM_THREADS = atoi(optarg);
			break;

		case 9:
			sleep(1);
			break;

		case 10:
			NUM_PREW = atoi(optarg);
			break;

		default:
			std::cerr << "invalid oi" << std::endl;
		}
		break;

	default:
		std::cerr << "getopt err" << std::endl;
		help();
	}
	argv += optind; argc -= optind;
	if(argc != 1) help();
	dbfile = argv[0];

	std::cout << "# benchprog: " << PROGNAME << std::endl;
	std::cout << "# dbfile: " << dbfile << std::endl;
	std::cout << "# comment: " << comment << std::endl;
	std::cout << "# sync: " << (do_sync ? "true" : "false") << std::endl;
	std::cout << "# intensive rebase: " << (do_intensiverebase ? "true" : "false") << std::endl;
	std::cout << "# random seq: " << (do_random ? "true" : "false") << std::endl;
	std::cout << "# workload: " << NUM_TX << " txs with " << NUM_R_PER_TX << " reads and " << NUM_W_PER_TX << " writes" << std::endl;

	NUM_KEYS = NUM_TX * NUM_W_PER_TX + NUM_PREW;
	keys = new int[NUM_KEYS];
	for(int i = 0; i < NUM_KEYS; ++ i)
	{
		keys[i] = i;
	}
	if(do_random) for(int i = 0; i < NUM_KEYS; ++ i)
	{
		std::swap(keys[i], keys[rand() % NUM_KEYS]);
	}

	std::cerr << "key init done. starting bench..." << std::endl;
	run_bench();

	delete[] keys;

	return 0;
}
