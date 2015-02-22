#
#
cd /home/cl/folding

curl http://fah-web.stanford.edu/daily_user_summary.txt.bz2 > daily_user_summary.txt.bz2

bunzip2 daily_user_summary.txt.bz2

afn="daily_user_summary"`date +_%Y%m%d_%H%M`".txt"
echo $afn

mv daily_user_summary.txt daily_user_summary`date +_%Y%m%d_%H%M`.txt

python FAHMM_DailyStatsFast.py -f $afn

bzip2 $afn
mv $afn "./archive/."

python FAHMM_UpdateToken.py

