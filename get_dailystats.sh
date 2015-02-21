#
#
cd /home/cl/folding

curl http://fah-web.stanford.edu/daily_user_summary.txt.bz2 > daily_user_summary.txt.bz2

bunzip2 daily_user_summary.txt.bz2

python FAHMM_DailyStatsFast.py -f daily_user_summary.txt

afn="daily_user_summary"`date +_%Y%m%d_%H%M`".txt"
echo $afn

mv daily_user_summary.txt daily_user_summary`date +_%Y%m%d_%H%M`.txt

bzip2 $afn


python FAHMM_UpdateToken.py

