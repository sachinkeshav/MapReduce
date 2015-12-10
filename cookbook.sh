#!/bin/bash
# Description: Cookbook to run hadoop jobs.
# Author: Sachin Kushwaha
# Date: December 9, 2015

HADOOP="hadoop"

JAR="target/MapReduce.jar"

allJobs="1 2 3 4"

printUsage(){
echo "
    sh cookbook.sh all              // run all jobs
    sh cookbook.sh list             // list the jobs    
    sh cookbook.sh jobs=2,3,4,7     // run jobs 2 3 4 and 7
"
}


# Runs a specific numbered job and checks for success or failure as well as displays the time for the job
runJob(){
	START=$(date +%s)
	case $1 in
		1)  $HADOOP jar $JAR -job WordCount -jobConfig classPath:jobconfig/wordcount/WordCount.jobcfg
		 	;;
#		2)  $HADOOP jar $JAR -job PairsApproach -jobConfig classPath:jobconfig/pairsapproach/PairsApproach.jobcfg
#		 	;;
#		3)  $HADOOP jar $JAR -job StripesApproach -jobConfig classPath:jobconfig/stripesapproach/StripesApproach.jobcfg
#		 	;;
#		4)  $HADOOP jar $JAR -job HybridApproach -jobConfig classPath:jobconfig/hybridapproach/HybridApproach.jobcfg
#		    ;;
	esac

	if  [ $? -eq 0 ]; then
		END=$(date +%s)
		DIFF=$(( $END - $START ))
		echo "$(printJobsNumber $1) completed in ${DIFF} seconds"
	else
		echo "$(printJobsNumber $1) was not completed succesfully"
		exit 1
	fi
}

printJobs(){
    for i in $allJobs; do
    	echo "$i) $(printJobsNumber $i)"
    done
}

printJobsNumber(){
case $1 in
1)  echo "Word Count"
    ;;
2)  echo "Pairs Approach"
    ;;
3)  echo "Stripes Approach"
    ;;
4)  echo "Hybrid Approach"
    ;;
*)
    echo "Unknown Job"
    ;;
esac
}

validateUserSpecifiedJobs(){
	if [ -z "$1" ] ; then
		echo "No jobs specified"
		exit 1
	elif [ ! $(echo "$1" | egrep '^[1-9][0-9]?(,[1-9][0-9]?)*$') ] ; then
		echo "The specified job $1 is not in valid format. Valid format 1,2,4,7"
		exit 1
	fi
}
if [ "$#" = "0" ]; then
	printUsage
	exit 1
fi

param=$(echo "$1" | tr "[:upper:]" "[:lower:]")

if [ $(echo "$param" | egrep '^jobs=[1-9][0-9]?(,[1-9][0-9]?)*$') ];  then
    userSpecifiedJob=$(echo $param | sed 's_jobs=__')
    param="jobs"
fi

echo "$0"

case "$param" in
	"all"	)	job=$allJobs
				;;
	"list"	)
				printJobs
				echo "Please specify a job number to run . eg 2 or 2,3,4 or 3-9 etc ?"
				read userSpecifiedJob
				validateUserSpecifiedJobs $userSpecifiedJob
				job=$(echo "$userSpecifiedJob" | sed 's_,_ _g')
				;;
	"jobs"	)
				validateUserSpecifiedJobs $userSpecifiedJob
				job=$(echo "$userSpecifiedJob" | sed 's_,_ _g')
				;;
	*		)
				printUsage
				exit 1
				;;
esac


startTime=$(date +%s)

echo "-------------------------------------------------------------------------"
echo "Processing started on $(date)."
echo "-------------------------------------------------------------------------"

for i in $job; do
	runJob $i;
done

endTime=$(date +%s)
overallTime=$(( $endTime-$startTime ))
echo "-------------------------------------------------------------------------"
echo "Processing completed on $(date). Took $overallTime seconds."
echo "-------------------------------------------------------------------------"
