# Geriatrix

Geriatrix is a simple, reproducible file system aging suite that induces
adequate amount of file and free space fragmentation by performing a series of
controlled file creates and deletes. Geriatrix is profile-driven and has 8
built-in aging profiles to facilitate aging.

Geriatrix has been [published in USENIX ATC 2018](http://cs.cmu.edu/~saukad/geriatrix.pdf).

## Building

Geriatrix requires the following to compile:
* a c++11 C++ compiler
* the Boost C++ libraries (http://www.boost.org/)

It should compile on most POSIX systems that have these prerequisite.

Geriatrix can be compiled with cmake (http://cmake.org/) or by
hand editing a basic Makefile.

To compile Geriatrix with cmake: make a build directory, change
to that directory, run cmake to generate the build, and then 
run "make" (and optionally "make install").  To set the 
installation directory, specify CMAKE_INSTALL_PREFIX on the 
command line.  If Boost is installed in a non-standard location,
specify it using CMAKE_PREFIX_PATH.

Here is an example of building Geriatrix to be installed in
/opt/bin using a Boost installed in /pkg:
```
cd geriatrix
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=/opt -DCMAKE_PREFIX_PATH=/pkg ..
make
make install
```

## Parameters

Geriatrix has a few parameters and setting them correctly is important to
ensure correct aging. The parameter definitions are:

- -n: size of your disk image in bytes (total partition size of file system)
- -u: fullness percentage of file system. Somewhere around 80% is reasonable.
  Note that this is a fractional value, i.e. 80% full would mean 0.8
- -r: random seed for a Geriatrix run
- -m: mount point without trailing / (eg /mnt and not /mnt/)
- -a: path to the age distribution file from the input aging profile
- -s: path to the size distribution file from the input aging profile
- -d: path to the directory depth distribution file from the input aging profile
- -x: path to write the output of age distribution of file system after aging
- -y: path to write the output of size distribution of file system after aging
- -z: path to write the output of dir depth distribution of file system after
  aging
- -t: number of threads you want to use for aging. This is a tricky parameter
  because multithreading might not be able to reproduce exact sequence of file
  creates / deletes because of the randomness in how the threads are scheduled
- -i: number of runs over the file system image size. 100 means that Geriatrix
  will execute until aging workload is equal to (100 * size of file system
  image)
- -f: fake mode. This is a mode to just test an aging profile without actually
  performing file creates or deletes. Essentially just data structure
  manipulation. Usually you should keep this value as 0 unless you are testing
  a new aging profile.
- -p: inject idle time between operations. This is a work-in-progress.
- -c: confidence interval. If you don't want to wait till perfect aging, you
  can specify a value between 0 and 1 for a notion of accuracy. 0.9 is aging
  done with upto 10% error. A value of 0 implies perfect aging.
- -q: query before quitting. Specifying 1 here will make Geriatrix ask you
  whether you want to continue aging beyond whatever has been performed.
- -w: running time limit in mins. Geriatrix will run till either -i parameter
  is reached or -w parameter is reached, whatever comes first. A -q value of 1
  will make Geriatrix wait for user input before quitting.
- -b: backend. Geriatrix supports multiple backends. This should be kept as
  "posix" (assuming you are benchmarking a posix compliant file system).

## Running
```
./bin/geriatrix -n 21474836480 -u 0.8 -r 42 -m /mnt -a ./profiles/agrawal/age_distribution.txt -s ./profiles/agrawal/size_distribution.txt -d ./profiles/agrawal/dir_distribution.txt -x /tmp/age.out -y /tmp/size.out -z /tmp/dir.out -t 1 -i 1000 -f 0 -p 0 -c 0 -q 1 -w 2880 -b posix
```
The above example shows a 20GB file system image being aged using the built-in
Agrawal aging profile. The file system is mounted at /mnt. Geriatrix has been
asked to stop aging after executing a workload of 20TB (1000 * FS image size)
or 2 days (2880 min), whichever comes first. Since the q parameter is 1,
Geriatrix will ask if aging should be continued after reaching one of these
thresholds. The c parameter being 0 specifies that Geriatrix will not quit
until it reaches perfect relative age distrubtion convergence (refer paper for
details about what this means).

## Output

After Geriatrix finishes aging (say using the above command), an output similar
to the following should be seen. Note that this is just an example. Your
numbers may be different:
```
Aging stopped because perfect convergence was achieved in input distributions.
============= OVERALL STATISTICS ===============
 Total runtime = 300.4352 mins.
 Total number of operations = 1485978
 Number of disk overwrites = 1000
 Total aging workload created = 20971520 MB
 Perfect convergence achieved
 Size distribution dumped in /tmp/size.out
 Dir depth distribution dumped in /tmp/dir.out
 Age distribution dumped in /tmp/age.out
================================================
```

## Contact

In case of issues or questions, please email saukad@cs.cmu.edu.
