# BigSorter

BigSorter.Generator - for generating large files.
BigSorter.Sorter - for sorting large files.

Both apps support help command (-h).

## BigSorter.Generator
Pretty straightforward. You can specify file name with -f param and target size in gigabytes with -g param.
I didn't enforce duplicate string as they happen naturally.

## BigSorter.Generator
I used external merge sort. You can specify file name with -f param and max parallel degree with -p param.
Make sure to specify -p param according to your processor cores. Default value is Environment.ProcessorCount - 1.
The app will estimate available RAM to allocate at the launch so freeing some memory by closing heavy apps migth help.

My specs: Core(TM) i7-12700H 2.70 GHz, 16,0 GB RAM, 1 TB SSD.
My results:
1GB ~ 30 sec.
10GB ~ 6 min 30 sec.
100GB ~ 1h 16 min.