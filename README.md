 # Measure Protocal Data Engineer Project
 
------------------------------------------------------------------------------

### Usage

Clone repository

`https://github.com/maafrank/Measure-Protocol-Assignment.git`

Navigate into directory

`cd Measure-Protocol-Assignment`

Run shell script which calls the python script with arguements. Currenly the only arguement is the input survey.csv. You can change path to --input. I will include the survey.csv in this repository so you can just run the following command.

`sh run_proj.sh`

------------------------------------------------------------------------------

### Notes

I used some libraries not directly specifed in the assignment page:

tqdm - used for progess bar of loops which take significant

time - used for sleep function while making api calls

argparse - used to pass in arguements so script is universal

If you have trouble with any of these libraries, 
I will refactor the code accordingly
