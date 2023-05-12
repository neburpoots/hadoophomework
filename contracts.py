
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
#split by ,
columns = 'ID,Name,Age,Photo,Nationality,Flag,Overall,Potential,Club,Club Logo,Value,Wage,Special,Preferred Foot,International Reputation,Weak Foot,Skill Moves,Work Rate,Body Type,Real Face,Position,Jersey Number,Joined,Loaned From,Contract Valid Until,Height,Weight,LS,ST,RS,LW,LF,CF,RF,RW,LAM,CAM,RAM,LM,LCM,CM,RCM,RM,LWB,LDM,CDM,RDM,RWB,LB,LCB,CB,RCB,RB,Crossing,Finishing,HeadingAccuracy,ShortPassing,Volleys,Dribbling,Curve,FKAccuracy,LongPassing,BallControl,Acceleration,SprintSpeed,Agility,Reactions,Balance,ShotPower,Jumping,Stamina,Strength,LongShots,Aggression,Interceptions,Positioning,Vision,Penalties,Composure,Marking,StandingTackle,SlidingTackle,GKDiving,GKHandling,GKKicking,GKPositioning,GKReflexes,Release Clause'.split(',')

class NoRatings(MRJob):
   def steps(self):
        return[
            MRStep(mapper=self.mapper_get_ratings,
                  reducer=self.reducer_count_ratings)
        ]
#Mapper function 
    def mapper_get_ratings(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           ratings=diction['Overall']
           #outputing as key value pairs
           yield ratings, 1
#Reducer function
   def reducer_count_ratings(self, key, values):
       yield key, sum(values)
if __name__ == "__main__":
    NoRatings.run()
