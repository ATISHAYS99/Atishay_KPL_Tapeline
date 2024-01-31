''' In this file we will read  '''
#Importing required dependencies
import pandas as pd
import numpy as np
import warnings
import datetime 
from datetime import datetime
import math
warnings.filterwarnings('ignore')
from ..Setups.Database.connection import Conn
from ..Setups.Loggers.Logger import Logs
import re


try:      
    conx = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=120.120.120.145; Database=Algo8; UID=kamransultan; PWD=sul@888tan;')
    print("Connection Successfully established!!")
except:
    print('Connection not established')


class Production:
    def __init__(self,engine,conx):
        self.engine = engine
        self.conx = conx
        self.Logs = Logs()
        
        self.production = pd.DataFrame()
        self.breakdown_df = pd.DataFrame()
        self.prod_data = []
        self.breakdown_data = []
        self.breakdown_duration = []
    
    def read_production_data (self):
        try :
            query = "SELECT * FROM [Algo8].[dbo].[TapeProduction] where PlantProd_DateTimeFrom > = '2023-01-01'"
            self.production = pd.read_sql(query, self.engine)
            self.production=self.production[['TapeId','MachineName','TapeDescription','TapeWidth','TapeDenier',
                                            'TapeColour','TapeName','ProdPerHour','PlantProd_DateTimeFrom','PlantProd_DateTimeTo']]
            self.production['TapeWidth'] = self.production['TapeWidth'].round(2)
            self.Logs.Logging("File :generate_schedule, Function : read_production_data, Status: executed")
            return self.production
        except Exception as e :
            print(e)
            self.Logs.Logging(f"File :generate_schedule, Function : read_production_data, Status: not executed, Reason : {e}")
        
    def process_production(self):
            
            #applying filter to production data according to min max threshold per machine
            production1 = self.production.loc[((self.production['MachineName']=='Tape plant 1') & (self.production['ProdPerHour']>=300) & (self.production['ProdPerHour']<=550))]
            production2 = self.production.loc[((self.production['MachineName']=='Tape plant 2') & (self.production['ProdPerHour']>=250) & (self.production['ProdPerHour']<=550))]

            production3 = self.production.loc[((self.production['MachineName']=='Tape plant 4') & (self.production['ProdPerHour']>=300) & (self.production['ProdPerHour']<=550))]
            production4 = self.production.loc[((self.production['MachineName']=='Tape plant 5') & (self.production['ProdPerHour']>=170) & (self.production['ProdPerHour']<=750))]

            production5 = self.production.loc[((self.production['MachineName']=='Tape plant 6') & (self.production['ProdPerHour']>=170) & (self.production['ProdPerHour']<=750))]

            production6 = self.production.loc[((self.production['MachineName']=='Tape plant 7') & (self.production['ProdPerHour']>=170) & (self.production['ProdPerHour']<=720))]
            production7 = self.production.loc[((self.production['MachineName']=='Tape plant 8') & (self.production['ProdPerHour']>=170) & (self.production['ProdPerHour']<=850))]
            production8 = self.production.loc[((self.production['MachineName']=='Tape plant 9') & (self.production['ProdPerHour']>=170) & (self.production['ProdPerHour']<=500))]
            self.prod_final = pd.concat([production1,production2,production3,production4,production5,production6,production7,production8])
            #adding filler values 
            query ="SELECT TapeId , Master_TapeRecipe_Percentage, Master_TapeRecipe_ApplicableFrom, MaterialName, IsRP FROM [Algo8].[dbo].[Master_TapeRecipe1] where MaterialName like '%Filler%'"
            df_filler = pd.read_sql(query, self.engine)
            df_filler.dropna(inplace=True)
            df_filler['Master_TapeRecipe_ApplicableFrom'] = pd.to_datetime(df_filler['Master_TapeRecipe_ApplicableFrom'])
            result = df_filler.sort_values('Master_TapeRecipe_ApplicableFrom', ascending=False).drop_duplicates('TapeId')[['TapeId', 'Master_TapeRecipe_Percentage']]
            result.rename(columns={'Master_TapeRecipe_Percentage':'TapeFiller'},inplace=True)
            self.prod_final = pd.merge(self.prod_final ,result,on='TapeId',how='left')
            #adding RP values
            query ="SELECT TapeId , Master_TapeRecipe_Percentage, Master_TapeRecipe_ApplicableFrom, MaterialName, IsRP FROM [Algo8].[dbo].[Master_TapeRecipe1] where MaterialName like '%REP%'"
            df_RP = pd.read_sql(query, self.engine)
            df_RP.dropna(inplace=True)
            df_RP['Master_TapeRecipe_ApplicableFrom'] = pd.to_datetime(df_RP['Master_TapeRecipe_ApplicableFrom'])
            result = df_RP.sort_values('Master_TapeRecipe_ApplicableFrom', ascending=False).drop_duplicates('TapeId')[['TapeId', 'Master_TapeRecipe_Percentage']]
            result.rename(columns={'Master_TapeRecipe_Percentage':'TapeRP'},inplace=True)
            self.prod_final= pd.merge(self.prod_final,result,on='TapeId',how='left')
            #adding color
            Tape_colour = self.prod_final['TapeColour']
            Tape_colour_updated = []
            for colour in Tape_colour:
                colour = colour.lower()
                if 'white' in colour:
                    Tape_colour_updated.append('white')
                else:
                    match = re.search(r'([a-zA-Z\s]+)', colour)
                    if match:
                        color_name = match.group(1).strip()
                        Tape_colour_updated.append(color_name)
            self.prod_final['TapeColour'] = Tape_colour_updated
            #adding uv
            TapeUV = []
            for index, row in self.prod_final.iterrows():
                tapename = row['TapeName']
                if tapename == 0:
                    TapeUV.append(0)
                    continue
                else :
                    pattern = r"NON UV"
                    match = re.search(pattern, tapename, re.IGNORECASE)
                    if match:
                        TapeUV.append(0)
                    else:
                        TapeUV.append(1)
            self.prod_final['TapeUv'] = TapeUV
            #adding width 
            Tape_width = []
            TapeWidth = self.prod_final['TapeWidth']
            for width in TapeWidth:
                if width < 2.5:
                    Tape_width.append('Narrow')
                elif 2.5 <= width <= 3.1:
                    Tape_width.append('Standard')
                else:
                    Tape_width.append('Wider')
            self.prod_final['Tape_Width'] = Tape_width
            #Defining the Denier to be low if it is <1000 else high
            Tape_Denier = []
            TapeDenier = self.prod_final['TapeDenier']
            for denier in TapeDenier:
                if denier <= 1000:
                    Tape_Denier.append('Low')
                else:
                    Tape_Denier.append('High')
            self.prod_final['Tape_Denier'] = Tape_Denier
            self.prod_final.fillna(0,inplace=True)

            return self.prod_final

class ProductionAnalyzer:

    def __init__(self, prod_df):
        self.production = prod_df
        self.velocity_df = pd.DataFrame()
        
  
    def calculate_velocity (self):
        prod_velocity = self.production.groupby(['MachineName','TapeDenier','TapeWidth','TapeColour','TapeFiller','TapeRP'])
        # mean production per hour
        self.velocity_df = prod_velocity.agg(
            Mean_ProdPerHour=pd.NamedAgg(column='ProdPerHour', aggfunc='mean'),
            Count=pd.NamedAgg(column='ProdPerHour', aggfunc='count')
        )
        
        self.velocity_df.reset_index(inplace=True)
        
        #categorising tape width and denier values
        Tape_width = []
        TapeWidth = self.velocity_df['TapeWidth']
        for width in TapeWidth:
            if width < 2.5:
                Tape_width.append('Narrow')
            elif 2.5 <= width <= 3.1:
                Tape_width.append('Standard')
            else:
                Tape_width.append('Wider')
        self.velocity_df['Tape_Width'] = Tape_width

        Tape_Denier = []
        TapeDenier = self.velocity_df['TapeDenier']
        for denier in TapeDenier:
            if denier <= 1000:
                Tape_Denier.append('Low')
            else:
                Tape_Denier.append('High')
        self.velocity_df['Tape_Denier'] = Tape_Denier
        
        #adding tape spec column denier width color filler RP
        self.velocity_df['TapeSpecs'] = self.velocity_df.apply(lambda row: f"[{row['Tape_Denier']} {row['Tape_Width']} {row['TapeColour']} {row['TapeFiller']} {row['TapeRP']}]", axis=1)
        #self.velocity_df.to_excel("Velocity_by_specs.xlsx",index=False)
        
    #Analyze velocity for each tape 
    def used_machine_velocity_by_tape_spec(self,Tape_spec):
        
        machine_and_velocity = []
        Tape_spec= [float(item) if isinstance(item, int) else item for item in Tape_spec]
        tape_spec_str = f"[{' '.join(map(str, Tape_spec))}]"
        
        unique_machine = self.velocity_df['MachineName'].unique()
        for machine in unique_machine:
            filtered_df = self.velocity_df.loc[(self.velocity_df['TapeSpecs'] == tape_spec_str)&(self.velocity_df['MachineName'] ==machine)] 
            if filtered_df.shape[0] > 0:
                machine_and_velocity.append([filtered_df['Mean_ProdPerHour'].values[0],machine])
            machine_and_velocity.sort(reverse = True) 
            
        return machine_and_velocity

    
    def last_production(self, tape_prod):
        last_prod = []
        for i in range(1, 10):
            if i ==3 :
                last_prod.append(tuple([' ',' ',' ',' ',' ',' ',' ',' ']))# no tape plant 3 values in production data
            else:    
                machine = f"Tape plant {i}"
                filtered_df = tape_prod.loc[tape_prod['MachineName'] == machine].copy()

                # Check if the DataFrame is empty
                if not filtered_df.empty:
                    filtered_df = filtered_df.sort_values('PlantProd_DateTimeFrom', ascending=False)
                    
                    # Access the first row
                    last_tape_and_description = filtered_df.iloc[0, filtered_df.columns.isin(['TapeId','TapeName','TapeDenier',
                                                                                            'TapeColour','TapeUv','TapeRP','TapeFiller',
                                                                                            'PlantProd_DateTimeTo','TapeWidth'])].values
                    
                    # Process color information
                    color_pattern = re.compile(r'\b([A-Za-z]+)(?=\(|\b)', re.IGNORECASE)
                    match = color_pattern.search(last_tape_and_description[3])
                    if match:
                        color = match.group(1).lower()
                        last_tape_and_description[3] = color

                    # Reorder and format the description
                    last_tape_and_description = list(last_tape_and_description)
                    last_tape_and_description.append(last_tape_and_description.pop(4))
                    last_tape_and_description = tuple(last_tape_and_description)

                    last_prod.append(last_tape_and_description)
                else:

                    continue

        return list(last_prod)
     
class Bestmachines:
    def __init__(self,last_production,analyzer,engine,conx):
        
        self.engine = engine
        self.conx = conx
        self.Logs = Logs()
        
        self.demand_df = pd.DataFrame()
        self.demand_df = pd.read_sql("SELECT * FROM [Algo8].[dbo].[Algo8_Tapeline_Demand]",self.engine)
        
        self.demand_df = self.demand_df.sort_values(['Tape_Target_Date','TapeDenier'])
        self.demand_df = self.demand_df.drop(['ULFabricBalanceToMake(Mtrs)','Production capacity per day(Mtrs)',
                                'Tape(Kg)',
                                'Tape_Load','Tape_Unload'
                                ],axis =1)

        self.demand_df = self.demand_df[self.demand_df['Total_Demand']!=0]
        
        self.tape_data_df = pd.DataFrame()
        
        self.tape_data_df = pd.read_sql("SELECT TapeId, TapeWidth, TapeDenier FROM [Algo8].[dbo].[Master_Tape]", self.engine)
        
        self.analyzer = analyzer
        self.plant_details = pd.read_excel('C:\Users\ATISHAY\Desktop\KPL_Tapeline\Tapeline\Dependencies\plant_details.xlsx',index_col=False)
        self.schedule = [['TapeId', 'FabricId','Tape_Completion_date','Loom_Location','LoomNo','LoomType',
                          'Tape_name','TapePropertyName','Seq','Total_Demand','Initial_Demand','Inventory_Utilised','Weft_Demand',
                          'Warp_Demand',
                          'Warp_rf_Demand',  'PreLogic_FinalFactor','Demand_Source','MaterialId', 'TapeFiller','TapeRP',
                          'TapeWidth', 'TapeColour', 'TapeDenier','TapeMarking', 
                          'FabricWarpMesh', 'Tape_Denier','Tape_Width', 'TapeUv','RPTAPE','tapetype','BestMachine','Velocity',
                          ]]
        self.machine_available_from = {}
        for i in range(1, 10):
            if i == 3:
                self.machine_available_from[f'Tape plant {i}'] =[]
            else:    
                self.machine_available_from[f'Tape plant {i}'] = [last_production[i-1][4].strftime('%Y-%m-%d %H:%M:%S')] ##datetime(2023, 6,27 ,14, 0, 0).strftime('%Y-%m-%d %H:%M:%S')
        
        self.not_sheduled = [['TapeId', 'FabricId','Tape_Completion_date','Loom_Location','LoomNo','LoomType',
                          'Tape_name','TapePropertyName','Seq','Total_Demand','Initial_Demand','Inventory_Utilised','Weft_Demand',
                          'Warp_Demand',
                          'Warp_rf_Demand',  'PreLogic_FinalFactor','Demand_Source','MaterialId', 'TapeFiller','TapeRP',
                          'TapeWidth', 'TapeColour', 'TapeDenier','TapeMarking', 
                          'FabricWarpMesh', 'Tape_Denier','Tape_Width', 'TapeUv','RPTAPE','tapetype',
                               'Reason']]
        self.machine_for_color_D19 = []
        self.machine_for_color_GR = []
        for i in range(1,10):
            if(last_production[i-1][3] != 'white' and last_production[i-1][3]!='White' and i!=3):
                if(i<3):
                    self.machine_for_color_D19.append(f'Tape plant {i}')    
                else:
                    self.machine_for_color_GR.append(f'Tape plant {i}')
    def get_machine_by_denier_width(self,denier,width):
        machine_and_velocity = []
        unique_machine = self.plant_details['MachineName'].unique()
        for machine in unique_machine:
            filtered_df = self.plant_details.loc[(self.plant_details['Tape_Denier'] == denier)&(self.plant_details['Tape_Width'] == width)&(self.plant_details['MachineName'] ==machine)] 
            if filtered_df.shape[0] > 0:
                machine_and_velocity.append([filtered_df['Mean_ProdPerHour'].values[0],machine])
            machine_and_velocity.sort(reverse = True) 
            
        return machine_and_velocity  
    def sort_tape_ids_by_nearest_width(self, target_id):
        tape_ids = list(self.tape_data_df['TapeId'])
        tape_widths = list(self.tape_data_df['TapeWidth'])
        target_index = tape_ids.index(target_id)
        target_width = tape_widths[target_index]
        pairs = zip(tape_ids, tape_widths)
        sorted_pairs = sorted(pairs, key=lambda pair: abs(pair[1] - target_width))
        sorted_tape_ids = [pair[0] for pair in sorted_pairs]
        return sorted_tape_ids

    def width_can_be_used(self,width,denier):

        if(width=='Narrow'):
            if(int(denier)<1000):
                return ['Tape plant 5','Tape plant 7']
            elif (int(denier)>=1000 and int(denier)<=1220):
                return ['Tape plant 5','Tape plant 6','Tape plant 7']   
            elif(int(denier)<=1700 and int(denier)>1220):
                return ['Tape plant 5','Tape plant 7','Tape plant 6','Tape plant 8']
            else:
                return ['Tape plant 5','Tape plant 8','Tape plant 6']
            
        elif(width=='Standard'):
            if(int(denier)<1000):
                return ['Tape plant 5','Tape plant 7']
            else:
                return ['Tape plant 4','Tape plant 7','Tape plant 9']
            
        else:
            if(int(denier)<1000):
                return ['Tape plant 5','Tape plant 7']
            else:
                return ['Tape plant 4','Tape plant 6','Tape plant 7','Tape plant 5','Tape plant 8']#   
      
    def schedule_tapes(self,analyzer):
        try :
            total_load = {'Tape plant 1':0,'Tape plant 2':0,'Tape plant 3':0,'Tape plant 4':0,'Tape plant 5':0,
                            'Tape plant 6':0,'Tape plant 7':0,'Tape plant 8':0,'Tape plant 9':0}
            
            #applying tape spec constraints
            for index,row in self.demand_df.iterrows():
                tape_id = row['TapeId']
                
                spec_columns = ['Tape_Denier', 'TapeWidth', 'TapeColour', 'TapeFiller', 'TapeRP']

                # Extract the tape specification values from the row
                tape_spec = [row[col] for col in spec_columns]
                

                machines = ['Tape plant 1','Tape plant 2','Tape plant 3','Tape plant 4','Tape plant 5',
                            'Tape plant 6','Tape plant 7','Tape plant 8','Tape plant 9']
                
                if(row['Loom_Location'] == 'D-19'):
                    
                        
                    machine_can_be_used = ['Tape plant 1','Tape plant 2']
                    machines = [element for element in machines if element in machine_can_be_used]

                    if(row['TapeColour'] != 'white'):
                            machine_can_be_used = self.machine_for_color_D19 
                    machines = [element for element in machines if element in machine_can_be_used]
                    
                    
                    if(len(machines)==0):
                        machines.append('Tape plant 2')
                        
                    self.machine_for_color_D19 = machines  
                
                    if (row['TapeWidth'] == 'Narrow' and int(row['TapeDenier'])>1500):
                        machine_can_be_used = ['Tape plant 1']
                        machines = [machine for machine in machines if machine in machine_can_be_used]

                    
                    elif (row['TapeWidth'] == 'Narrow' and int(row['TapeDenier'])<940):
                        machine_can_be_used = ['Tape plant 2']
                        machines = [machine for machine in machines if machine in machine_can_be_used]
                        
                    elif (row['TapeWidth'] == 'Narrow' and int(row['TapeDenier'] >=940  and int(row['TapeDenier'])<=1500)):
                        machine_can_be_used = ['Tape plant 2']
                        machines = [machine for machine in machines if machine in machine_can_be_used]
                        
                    elif (row['TapeWidth']== 'Wider' and int(row['TapeDenier']>=1000)):
                        machine_can_be_used = ['Tape plant 1']#2
                        machines = [machine for machine in machines if machine in machine_can_be_used]
                        
                    elif (int(row['TapeDenier']<1000) and int(row['RPTAPE'])==0 ):
                        machine_can_be_used = ['Tape plant 2']
                        machines = [element for element in machines if element in machine_can_be_used]

                        
                    elif(int(row['TapeDenier']<1000) and int(row['RPTAPE'])==1 ):
                        self.not_sheduled.append(row.values.tolist() + ['No suggested machine (RP)'])
                        continue

                    elif (row['TapeDenier']<1000 and row['TapeFiller']>=10):
                    
                        machine_can_be_used=['Tape plant 2']
                        machines = [element for element in machines if element in machine_can_be_used]
                        
                    elif (row['TapeDenier']<1000 and row['TapeFiller']< 10):
                        machine_can_be_used = []
                        machines = [element for element in machines if element in machine_can_be_used]    

                        if(len(machines)==0):
                            self.not_sheduled.append(row.values.tolist() + ['Filler_required_D-19 (Transfer to Gajner)'])
                            continue
                        
                    if(len(machines)==0):

                        self.not_sheduled.append(row.values.tolist() + ['No machine suggested for specification'])
                        continue
                        

                else:

                        
                    machine_can_be_used = ['Tape plant 4','Tape plant 5','Tape plant 6','Tape plant 7','Tape plant 8','Tape plant 9']    
                    machines = [element for element in machines if element in machine_can_be_used]
                    if(row['TapeColour'] != 'white'):
                        machine_can_be_used = self.machine_for_color_GR
                            
                    machines = [element for element in machines if element in machine_can_be_used]
                    if(len(machines)==0):
                        
                        if(row['TapeRP']<100):
                            machines  = ['Tape plant 5','Tape plant 7']
                        else:
                            machines  = ['Tape plant 7','Tape plant 8']
                            

                    self.machine_for_color_GR = machines
                    if(row['tapetype'] =='Webbing'):
                        machine_can_be_used = ['Tape plant 6','Tape plant 7','Tape plant 8']
                        machines = [element for element in machines if element in machine_can_be_used]
                    
                    elif(row['tapetype'] == 'Fabrilated'):
                        machine_can_be_used = ['Tape plant 9']
                        machines = [element for element in machines if element in machine_can_be_used]
                        
                    if (int(row['TapeDenier'])<1000) and (int(row['RPTAPE'])==1):
                        machine_can_be_used = ['Tape plant 7']
                        machines = [element for element in machines if element in machine_can_be_used]
                        
                    elif (int(row['TapeDenier'])>=1000 and int(row['RPTAPE']==1)):
                        machine_can_be_used = ['Tape plant 4','Tape plant 5','Tape plant 7','Tape plant 6','Tape plant 8']  #5->4 6
                        machines = [element for element in machines if element in machine_can_be_used]
                        
                    elif (int(row['TapeDenier'])<1000) and (int(row['RPTAPE'])==0):
                        machine_can_be_used = ['Tape plant 5','Tape plant 7'] 
                        machines = [element for element in machines if element in machine_can_be_used]   
                        
                    elif (int(row['TapeDenier'])>=1000) and (int(row['RPTAPE'])==0):
                        machine_can_be_used = ['Tape plant 5','Tape plant 4','Tape plant 6','Tape plant 8'] #7
                        machines = [element for element in machines if element in machine_can_be_used]  
                        
                    elif (row['TapeFiller'] >10 and int(row['TapeDenier'])>=1000):
                        machine_can_be_used = ['Tape plant 6','Tape plant 7','Tape plant 8'] 
                        machines = [element for element in machines if element in machine_can_be_used]
                        
                    elif (row['TapeFiller'] <=10 and int(row['TapeDenier'])>=1000):
                        machine_can_be_used = ['Tape plant 5','Tape plant 4']
                        machines = [element for element in machines if element in machine_can_be_used]
                        
                    elif(row['TapeFiller'] >10 and int(row['TapeDenier'])<1000):  
                        machine_can_be_used = ['Tape plant 5']
                        machines = [element for element in machines if element in machine_can_be_used]
                        

                    
                    machine_can_be_used = self.width_can_be_used(row['TapeWidth'],row['TapeDenier'])
                    machines = [element for element in machines if element in machine_can_be_used]

                    
                    if(len(machines)==0):
                        self.not_sheduled.append(row.values.tolist() + ['No machine suggested for specification'])
                        continue
                    
                #get eligible machines' velocity from tape spec using velocity data                
                used_machine = analyzer.used_machine_velocity_by_tape_spec(tape_spec)

                used_machine = [element for element in used_machine if element[1] in machines]
                
                
                
                if (len(used_machine)==0):
                    used_machine = self.get_machine_by_denier_width(row['Tape_Denier'],row['TapeWidth'])#function to get machine from tapeline design data
                    used_machine = [element for element in used_machine if element[1] in machines]
                    

                
                if (len(used_machine)==0):
                    self.not_sheduled.append(row.values.tolist() + ['No_machine_for_this_tape_spec(Velocity)'])
                    continue
                
                
                    
                earliest_complete = []
                for machine in used_machine:
                    starttime = datetime.strptime(self.machine_available_from[machine[1]][0], '%Y-%m-%d %H:%M:%S') 
                    total_demand = row['Total_Demand']
                    time_taken = (total_demand) / machine[0]
                    time_taken = time_taken*60
                    completion_time = starttime + pd.DateOffset(minutes=time_taken)
                    earliest_complete.append([completion_time, starttime,machine[1],machine[0]])

                earliest_complete.sort()

                best_machine = None
                best_vel = 0
                machine_avl_time = 0
                min_load = float('inf')
                
                for completion_info in earliest_complete:
                    machine_name = completion_info[2]
                    machine_vel = completion_info[3]
                    comp_time = completion_info[0]
                    machine_load = total_load.get(machine_name, 0)

                    # Check if the current load plus new demand stays under limit
                    if machine_load + row['Total_Demand'] <= 75000:
                        # Choose the machine with the minimum load under the limit
                        if machine_load < min_load:
                            min_load = machine_load
                            best_machine = machine_name   
                            best_vel = machine_vel
                            machine_avl_time = comp_time

                if best_machine is not None:
                    # Assign the best machine
                    total_load[best_machine] += row['Total_Demand']
                    
                    self.machine_available_from[best_machine] = [machine_avl_time.strftime('%Y-%m-%d %H:%M:%S')]
                    
                    self.schedule.append(row.values.tolist()+ [best_machine,best_vel])

                else:
                    # If no machine meets the criteria, add to not scheduled
                    self.not_sheduled.append(row.values.tolist() + ['Load exceeds or no machine available'])
            
                self.Logs.Logging("File :generate_schedule, Function : best_machine_schedule_tapes, Status: executed")
                return [self.schedule,self.not_sheduled]
        except Exception as e :
            print(e)
            self.Logs.Logging(f"File :generate_schedule, Function :  best_machine_schedule_tapes, Status: not executed, Reason : {e}")
                      
class TapeScheduler:
    def __init__(self, schedule_df,last_production,engine):
        self.engine = engine
        self.conx = conx
        self.Logs = Logs()
        self.schedule_df = schedule_df
        self.changeover_df = pd.DataFrame()
        self.planned_stop_data = pd.DataFrame()
        self.tape_data_df = pd.DataFrame()
        self.denier_step_data = pd.DataFrame()
        self.recipe_change_data = pd.DataFrame()
        
        self.changeover_df = pd.read_sql('SELECT * FROM [Algo8].[dbo].[Tapeline_Changeover_Data]',self.engine)
        
        self.denier_step_data = pd.read_sql('SELECT [TapeType], [DenierChange(1Step)], [Duration in Minutes] FROM [Algo8].[dbo].[Tapeline_ConfiguredRule_DenierStepUpDown]', self.engine)

        self.planned_stop_data = pd.read_sql('SELECT [Tapeline],[StartTime],[EndTime] FROM [Algo8].[dbo].[Tapeline_PlannedStop]',self.engine)

        self.recipe_change_data = pd.read_sql('SELECT [Recipe1],[Recipe2],[Duration in Minutes] FROM [Algo8].[dbo].[Tapeline_ConfiguredRule_RecipeChange]',self.engine)
                
        self.tape_data_df = pd.read_sql("SELECT TapeId, TapeWidth, TapeDenier FROM [Algo8].[dbo].[Master_Tape]", self.engine)
        
        self.regular_tapes = pd.read_excel('C:\Users\ATISHAY\Desktop\KPL_Tapeline\Tapeline\Dependencies\Regular Tape (1).xlsx',index_col=False)
        self.schedule = [['Prev_Tape_Id', 'Prev_Tape_name','Prev_Tape_Denier' ,'Prev_Tape_Colour','Tape_id', 
                          'Tape_name','TapePropertyName','Seq','Tape_Denier','Tape_Width'
                          ,'TapeColour',
                          'TapeMarking','Demand_Source','Total_Demand','Weft_demand','Warp_Demand','Warp_rf_Demand',
                          'Tape_demand + Production loss + Wastage',
                          'Target_date', 'Machine','Plant_Location','LoomNo','LoomType','PreLogic_FinalFactor','TapeDenier_Change_Starttime',
                          'TapeDenier_Change_Endtime',
                          'Recipe_change_Starttime','Recipe_change_Endtime','Width_Change_Starttime','Width_Change_Endtime','Prod_Start_Time', 'Prod_End_time', 'Plan no.', 
                          'Type of Changeover', 'Production_loss (kg)', 'Wastage (kg)', 'Man power loss (Rs)']]
        self.machine_available_from = {}
        for i in range(1, 9):
            if i==3:
                self.machine_available_from[f'Tape plant {i}']= []
            else:
                self.machine_available_from[f'Tape plant {i}'] = [last_production[i-1][0], last_production[i-1][8],last_production[i-1][2], 
                                                                last_production[i-1][4].strftime('%Y-%m-%d %H:%M:%S'),1,
                                                                last_production[i-1][3],last_production[i-1][5],last_production[i-1][6],last_production[i-1][7],last_production[i-1][1]]
    def get_changeover_data(self, prev_tape_id, tape_id, machine):
        if prev_tape_id == 0:
            return ['no changeover', 0, 0, 0, 0]
        else:
            filter1 = self.schedule_df.loc[(self.schedule_df['TapeId'] == prev_tape_id)].copy()
            filter2 = self.schedule_df.loc[(self.schedule_df['TapeId'] == tape_id)].copy() 
            filter1.reset_index(drop=True, inplace=True)
            filter2.reset_index(drop=True, inplace=True)
            if (not filter1['MaterialId'].equals(filter2['MaterialId'])) and (not filter1['FabricWarpMesh'].equals(filter2['FabricWarpMesh'])):
                changeover = 'recipe+mesh change'
            elif (not filter1['MaterialId'].equals(filter2['MaterialId'])) and (not filter1['TapeWidth'].equals(filter2['TapeWidth'])):
                changeover = 'recipe+cam change'
            elif (not filter1['TapeColour'].equals(filter2['TapeColour'])) and (not filter1['FabricWarpMesh'].equals(filter2['FabricWarpMesh'])):
                changeover = 'color+mesh change'
            elif (not filter1['TapeColour'].equals(filter2['TapeColour'])) and (not filter1['TapeWidth'].equals(filter2['TapeWidth'])):
                changeover = 'color +cam change'
            elif (not filter1['FabricWarpMesh'].equals(filter2['FabricWarpMesh'])):
                changeover = 'Mesh change'
            elif (not filter1['TapeColour'].equals(filter2['TapeColour'])) and (filter2['TapeColour'].equals('white')):
                changeover = 'Color change (Color to white)'
            elif (not filter1['TapeColour'].equals(filter2['TapeColour'])) and (filter1['TapeColour'].equals('white')):
                changeover = 'Color change (white to color )'
            elif (not filter1['MaterialId'].equals(filter2['MaterialId'])):
                changeover = 'Recipe change (filler)'
            elif (not filter1['TapeWidth'].equals(filter2['TapeWidth'])):
                changeover = 'Cam Change /Spacer change'
            else:
                changeover = 'No Changeover'
            change_data = self.changeover_df.loc[(self.changeover_df['Tape line no'] == machine) & (self.changeover_df['Type of changeover'] == changeover)]
            if len(change_data.values.tolist()) == 0:
                return [changeover, 0, 0, 0, 0]
            else:
                return change_data.values.tolist()[0][1:]

    def filter_with_danier(self,tape_denier,machine_can_be_used):
        filtered_machines = []
        for machine in machine_can_be_used:
            prev_tape_denier = self.machine_available_from[machine][2]
            if(machine in self.step_down):
                if(tape_denier<prev_tape_denier):
                    filtered_machines.append(machine)
            else:
                if(tape_denier>=prev_tape_denier):
                    filtered_machines.append(machine)
        return filtered_machines

    def get_denier_changetime(self,machine,tapedenier,width):
        if(tapedenier>1000 and width in ['Narrow','Wider']):
            prev_tape_denier = self.machine_available_from[machine][2]
            
            time = math.ceil((abs(tapedenier-prev_tape_denier))/150) * 30 
        else:
            time = 0     
        return time 
    def get_recipe_changetime(self,machine,tape_UV,tape_Colour,tape_Filler):
        prev_tape_UV = self.machine_available_from[machine][6]
        prev_tape_colour = self.machine_available_from[machine][5]
        prev_tape_Filler = self.machine_available_from[machine][8]
        total_time = 0
        
        if (prev_tape_UV ==1 and tape_UV == 0):
            time = self.recipe_change_data[(self.recipe_change_data['Recipe1'] == 'UV') & (self.recipe_change_data['Recipe2']=='Non UV')]['Duration in Minutes'].values[0]
            total_time+= time
        if (prev_tape_UV ==0 and tape_UV == 1):
            time = self.recipe_change_data[(self.recipe_change_data['Recipe1'] == 'Non UV') & (self.recipe_change_data['Recipe2']=='UV')]['Duration in Minutes'].values[0]
            total_time+= time
        if (prev_tape_Filler >10 and tape_Filler <=10):
            time = self.recipe_change_data[(self.recipe_change_data['Recipe1'] == 'Filler More Than 10%') & (self.recipe_change_data['Recipe2']=='Filler less Than 10%')]['Duration in Minutes'].values[0]
            total_time+= time
        if (prev_tape_Filler <10 and tape_Filler >=10):
            time = self.recipe_change_data[(self.recipe_change_data['Recipe1'] == 'Filler less Than 10%') & (self.recipe_change_data['Recipe2']=='Filler More Than 10%')]['Duration in Minutes'].values[0]
            total_time+= time
        if (prev_tape_colour=='White' and tape_Colour!='White'):
            time = self.recipe_change_data[(self.recipe_change_data['Recipe1'] == 'White') & (self.recipe_change_data['Recipe2']=='Color')]['Duration in Minutes'].values[0]
            total_time+= time
        if (prev_tape_colour!='White' and tape_Colour=='White'):
            time = self.recipe_change_data[(self.recipe_change_data['Recipe1'] == 'Color') & (self.recipe_change_data['Recipe2']=='White')]['Duration in Minutes'].values[0]
            total_time+= time
            
        return int(total_time)
    
    def get_denier_changetime_step(self,machine,tapedenier,width):
        time = 0
        if(tapedenier>1000):
            prev_tape_denier = self.machine_available_from[machine][2]

            if width == 'Wider':
                step = self.denier_step_data[self.denier_step_data['TapeType'] == 'Wider']['DenierChange(1Step)'].values[0]
                duration = self.denier_step_data[self.denier_step_data['TapeType'] == 'Wider']['Duration in Minutes'].values[0]
                
                if abs(tapedenier-prev_tape_denier)<250:
                    time = 0
                else :
                    time = math.ceil((abs(tapedenier-prev_tape_denier))/int(step)) * int(duration) 

            if width == 'Narrow':
                step = self.denier_step_data[self.denier_step_data['TapeType'] == 'Narrow']['DenierChange(1Step)'].values[0]
                duration = self.denier_step_data[self.denier_step_data['TapeType'] == 'Narrow']['Duration in Minutes'].values[0]
                
                if abs(tapedenier-prev_tape_denier)<250:
                    time = 0
                else :
                    time = math.ceil((abs(tapedenier-prev_tape_denier))/int(step)) * int(duration)
 
        elif (tapedenier <=1000):
            prev_tape_denier = self.machine_available_from[machine][2]

            step = self.denier_step_data[self.denier_step_data['TapeType'] == 'Low Denier']['DenierChange(1Step)'].values[0]
            duration = self.denier_step_data[self.denier_step_data['TapeType'] == 'Low Denier']['Duration in Minutes'].values[0]
            if abs(tapedenier-prev_tape_denier)<250:
                time = 0
            else :
                time = math.ceil((abs(tapedenier-prev_tape_denier))/int(step)) * int(duration)
  
        else:
            time = 0
        
        return time 

    def find_nearest_denier(self,prev_value, values_list):
        prev_value = prev_value
        nearest_denier = []
        while(len(values_list)!=0):
            nearest_value = min(values_list, key=lambda x: abs(x - prev_value))
            nearest_denier.append(nearest_value) 
            prev_value  = nearest_value 
            values_list.pop(values_list.index(nearest_value))
        return nearest_denier
    
    def get_width_changetime(self,prev_tape_width,tape_width):
        time= 0
        if prev_tape_width != tape_width :
            time = 40
        return int(time)    
    
    def schedule_tapes(self,last_production):
        
        try :
            for i in range(1,10): 
                if i==3:
                    continue
                machine = f'Tape plant {i}'
                
                ps_starttime_values =self.planned_stop_data[self.planned_stop_data['Tapeline'] ==f'Tapeline {i}']['StartTime'].values
                ps_endtime_values = self.planned_stop_data[self.planned_stop_data['Tapeline'] ==f'Tapeline {i}']['EndTime'].values
                ps_starttime  = 0
                ps_endtime = 0 

                if(last_production[i-1][3] !='white'):
                    color = 1
                else:
                    color = 0  

                if(color == 1):
                    filtered_schedule = self.schedule_df.loc[(self.schedule_df['BestMachine'] == machine)]
                    
                    unique_completion = filtered_schedule['Tape_Completion_date'].unique()
                    
                    for completion in unique_completion: 
                        filtered_schedule1 =  filtered_schedule.loc[filtered_schedule['Tape_Completion_date']==completion]
                        
                        unique_denier = filtered_schedule1['TapeDenier'].unique() 
                        desired_order = self.find_nearest_denier(self.machine_available_from[machine][2],list(unique_denier))
                        filtered_schedule2 = filtered_schedule1.sort_values(by='TapeDenier', key=lambda x: x.map(dict(zip(desired_order, range(len(desired_order))))))
                        for index,row in filtered_schedule2.iterrows():  
                            tape_id = row['TapeId']
                            tape_name = row['Tape_name']
                            tape_denier = row['TapeDenier']
                            tape_UV = row['TapeUv']
                            tape_Colour = row['TapeColour']
                            tape_Filler = row['TapeFiller']
                            
                            
                            #colored tapes of this machine
                            if(row['TapeColour'] != 'white'): 
                                if (color ==2):
                                    continue
                                else :
                                
                                    filtered_schedule3 = filtered_schedule.loc[(filtered_schedule['TapeColour'] != 'white')]
                                    
                                    unique_color = filtered_schedule3['TapeColour'].unique()    
                                    for colour in unique_color: 
                                        filtered_schedule4 =  filtered_schedule3.loc[filtered_schedule3['TapeColour']==colour]
                                        unique_completion = filtered_schedule4['Tape_Completion_date'].unique()
                                        
                                        for completion in unique_completion: 
                                            filtered_schedule5 =  filtered_schedule4.loc[filtered_schedule4['Tape_Completion_date']==completion]
                                            unique_denier = filtered_schedule4['TapeDenier'].unique()
                                            desired_order = self.find_nearest_denier(self.machine_available_from[machine][2],list(unique_denier))
                                            filtered_schedule6 = filtered_schedule5.sort_values(by='TapeDenier', key=lambda x: x.map(dict(zip(desired_order, range(len(desired_order))))))
                                            
                                            for index,row in filtered_schedule6.iterrows():
                                                
                                                tape_id = row['TapeId']
                                                tape_name = row['Tape_name']
                                                tape_denier = row['TapeDenier']
                                                tape_UV = row['TapeUv']
                                                tape_Colour = row['TapeColour']
                                                tape_Filler = row['TapeFiller']
                                                tape_width = row['Tape_Width']
                                                prev_tape_id = self.machine_available_from[machine][0]
                                                prev_tape_denier = self.machine_available_from[machine][2]
                                                prev_tape_width = self.machine_available_from[machine][9]
                                                
                                                
                                                width_change_starttime = 0
                                                width_change_endtime = 0
                                                recipe_change_starttime = 0
                                                recipe_change_endtime = 0
                                                denier_change_starttime = 0
                                                denier_change_endtime = 0
                                                time_taken = 0
                                                total_demand = 0
                                                completion_time = 0
                                                changeover_data = self.get_changeover_data(prev_tape_id, tape_id, machine)
                                                
                                                
                                                width_changetime = self.get_width_changetime(prev_tape_width,tape_width)
                                                recipe_changetime = self.get_recipe_changetime(machine,tape_UV,tape_Colour,tape_Filler)
                                                
                                                #width changeover
                                                if width_changetime!=0 :
                                                    print(width_changetime)
                                                    width_change_starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                                    width_change_endtime = width_change_starttime + pd.DateOffset(minutes=width_changetime)
                                                    starttime = width_change_endtime #production start time
                                                    total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                                    time_taken = (total_demand) / row['Velocity']
                                                    time_taken = round(time_taken * 60)
                                                    completion_time = starttime + pd.DateOffset(minutes=time_taken)#
                                                
                                                
                                                elif recipe_changetime !=0:
                                                    recipe_change_starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                                    
                                                    
                                                    recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                                    starttime = recipe_change_endtime #production start time
                                                    total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                                    time_taken = (total_demand) / row['Velocity']
                                                    time_taken = round(time_taken * 60)
                                                    completion_time = starttime + pd.DateOffset(minutes=time_taken)#+ pd.DateOffset(minute=time_taken)

                                                    if len(ps_starttime_values) > 0 and len(ps_endtime_values) > 0:
                                                        ps_starttime = pd.Timestamp(ps_starttime_values[0])
                                                        ps_endtime = pd.Timestamp(ps_endtime_values[0])
                                                        if (recipe_change_starttime > ps_starttime) & (completion_time < ps_endtime):
                                                                
                                                            recipe_change_starttime = ps_endtime
                                                            recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                                            starttime = recipe_change_endtime
                                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                        elif (recipe_change_starttime < ps_starttime) & (completion_time < ps_endtime) & (completion_time >ps_starttime):
                                                                
                                                            recipe_change_starttime = ps_endtime
                                                            recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                                            starttime = recipe_change_endtime
                                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                        elif (recipe_change_starttime < ps_starttime) & (completion_time > ps_endtime):

                                                                
                                                            recipe_change_starttime = ps_endtime
                                                            recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                                            starttime = recipe_change_endtime
                                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                        elif (recipe_change_starttime > ps_starttime) & (completion_time > ps_endtime)&(recipe_change_starttime<ps_endtime):

                                                                
                                                            recipe_change_starttime = ps_endtime
                                                            recipe_change_endtime = recipe_change_starttime+ pd.DateOffset(minutes=recipe_changetime)
                                                            starttime = recipe_change_endtime
                                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                else:
                                                    denier_changetime = self.get_denier_changetime_step(machine,tape_denier,row['TapeWidth'])
                                                    if denier_changetime == 0 :
                                                        starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                                        total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                                        time_taken = (total_demand) / row['Velocity']
                                                        time_taken = round(time_taken * 60)
                                                        completion_time = starttime + pd.DateOffset(minutes=time_taken)#pd.to_timedelta(time_taken,unit='h')
                                                    else:                                          
                                                        s = ((row['TapeDenier']-prev_tape_denier)/250) 
                                                        if s<0 :
                                                            t = abs(math.floor(s))
                                                        else :
                                                            t= abs(math.ceil(s))

                                                        denier = prev_tape_denier
                                                        tape_width = row['Tape_Width']
                                                        
                                                        st_time = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                                        e_time = 0
                                                        if t!=1:
                                                            for i in range(1,t+1):
                                                                name = 'Denier_Step'
                                                                id = 0 
                                                                demand=0
                                                                #step up
                                                                if (denier <=row['TapeDenier']):
                                                                    if(s>0):
                                                                        denier = denier + 250
                                                                    den = self.regular_tapes['Tape_Denier'].values
                                                                    filtered_rows = pd.DataFrame()
                                                                    if (denier >row['TapeDenier']):
                                                                        break
                                                                    if (denier in den) :
                                                                        filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == denier)
                                                                                                        & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                                        &(tape_width <= self.regular_tapes['Max Tape Width'])]                                                                                      
                                                                        if not filtered_rows.empty:
                                                                            name = filtered_rows['TapeName'].values[0]
                                                                            id = filtered_rows['TapeId'].values[0]
                                                                            demand = row['Velocity']*0.5
                    
                                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                            self.machine_available_from[machine][2],0,
                                                                                            id,name,0,0, denier,0,0,0,0,
                                                                                            demand,0,0,0,
                                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                                            row['LoomType'],
                                                                                            0,
                                                                                            0,0,0,0,0,0,
                                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                                            0,0,0,0])
                                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                                            st_time = e_time 
                                                                            
                                                                    elif denier not in den or filtered_rows.empty==True :
                                                                        
                                                                        denn = [x for x in den if (denier-250 <x )& (denier >x)]
                                                                        closest_denier = min(denn , key=lambda x: abs(x - denier))
                                                                        print("Closeest_denier1:",closest_denier)
                                                                        if closest_denier < row['TapeDenier']:
                                                                            filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == closest_denier )
                                                                                                    & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                                    &(tape_width <= self.regular_tapes['Max Tape Width'])]          
                                                                        if not filtered_rows.empty:
                                                                            name = filtered_rows['TapeName'].values[0]
                                                                            id = filtered_rows['TapeId'].values[0]
                                                                            demand = row['Velocity']*0.5
                                                                            denier = closest_denier
                                                                            print("Closest_denier : ",denier)
                    
                                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                            self.machine_available_from[machine][2],0,
                                                                                            id,name,0,0, denier,0,0,0,0,
                                                                                            demand,0,0,0,
                                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                                            row['LoomType'],
                                                                                            0,
                                                                                            0,0,0,0,0,0,
                                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                                            0,0,0,0])
                                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                                            st_time = e_time 
                                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                                            0,0,
                                                                            0,0,0,0]
                                                                            
                                                                        else:
                                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                            self.machine_available_from[machine][2],0,
                                                                                            id,name,0,0, denier,0,0,0,0,
                                                                                            demand,0,0,0,
                                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                                            row['LoomType'],
                                                                                            0,
                                                                                            st_time,st_time + pd.DateOffset(minutes=30),0,0,
                                                                                            0,0,
                                                                                            0,0,0, 
                                                                                            0,0,0,0])                                    

                                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                                            st_time = e_time 
                                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                                            0,0,
                                                                            0,0,0,0]
                                                                #step down
                                                                if (denier >=row['TapeDenier']) :
                                                                            
                                                                    if(s<0):
                                                                        denier = denier - 250
                                                                    den = self.regular_tapes['Tape_Denier'].values
                                                                    filtered_rows = pd.DataFrame()
                                                                    if (denier <row['TapeDenier']):
                                                                        break
                                                                    if (denier in den) :
                                                                        filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == denier)
                                                                                                        & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                                        &(tape_width <= self.regular_tapes['Max Tape Width'])]                                                                                      
                                                                        if not filtered_rows.empty:
                                                                            name = filtered_rows['TapeName'].values[0]
                                                                            id = filtered_rows['TapeId'].values[0]
                                                                            demand = row['Velocity']*0.5
                    
                                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                            self.machine_available_from[machine][2],0,
                                                                                            id,name,0,0, denier,0,0,0,0,
                                                                                            demand,0,0,0,
                                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                                            row['LoomType'],
                                                                                            0,
                                                                                            0,0,0,0,0,0,
                                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                                            0,0,0,0])
                                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                                            st_time = e_time 
                                                                            
                                                                    elif denier not in den or filtered_rows.empty==True :
                                                                        
                                                                        denn = [x for x in den if (denier-250 <x )& (denier >x)]
                                                                        closest_denier = min(denn , key=lambda x: abs(x - denier))
                                                                        print("Closeest_denier1:",closest_denier)
                                                                        if closest_denier >row['TapeDenier']:
                                                                            filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == closest_denier )
                                                                                                    & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                                    &(tape_width <= self.regular_tapes['Max Tape Width'])]          
                                                                        if not filtered_rows.empty:
                                                                            name = filtered_rows['TapeName'].values[0]
                                                                            id = filtered_rows['TapeId'].values[0]
                                                                            demand = row['Velocity']*0.5
                                                                            denier = closest_denier
                                                                            print("Closest_denier : ",denier)
                    
                                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                            self.machine_available_from[machine][2],0,
                                                                                            id,name,0,0, denier,0,0,0,0,
                                                                                            demand,0,0,0,
                                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                                            row['LoomType'],
                                                                                            0,
                                                                                            0,0,0,0,0,0,
                                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                                            0,0,0,0])
                                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                                            st_time = e_time 
                                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                                            0,0,
                                                                            0,0,0,0]
                                                                            
                                                                        else:
                                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                            self.machine_available_from[machine][2],0,
                                                                                            id,name,0,0, denier,0,0,0,0,
                                                                                            demand,0,0,0,
                                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                                            row['LoomType'],
                                                                                            0,
                                                                                            st_time,st_time + pd.DateOffset(minutes=30),0,0,
                                                                                            0,0,0, 0,0,
                                                                                            0,0,0,0])                                    

                                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                                            st_time = e_time 
                                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                                            0,0,
                                                                            0,0,0,0]
                                                                        
                                                        denier_change_starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))

                                                        denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                        starttime = denier_change_endtime
                                                        total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                                        time_taken = (total_demand) / row['Velocity']
                                                        time_taken = round(time_taken * 60)
                                                        completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                        if len(ps_starttime_values) > 0 and len(ps_endtime_values) > 0:
                                                            ps_starttime = pd.Timestamp(ps_starttime_values[0])
                                                            ps_endtime = pd.Timestamp(ps_endtime_values[0])
                                                            if (denier_change_starttime > ps_starttime) & (completion_time < ps_endtime):
                                                                    
                                                                denier_change_starttime = ps_endtime
                                                                
                                                                denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                                starttime = denier_change_endtime
                                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                                
                                                            elif (denier_change_starttime < ps_starttime) & (completion_time < ps_endtime)& (completion_time >ps_starttime):
                                                                    
                                                                denier_change_starttime = ps_endtime
                                                                denier_change_endtime =  denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                                starttime = denier_change_endtime
                                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                            elif (denier_change_starttime < ps_starttime) & (completion_time > ps_endtime):
                                                                    
                                                                denier_change_starttime = ps_endtime
                                                                
                                                                denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                                starttime = denier_change_endtime
                                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                            elif (denier_change_starttime > ps_starttime) & (completion_time > ps_endtime)& (denier_change_starttime<ps_endtime):
                                                                    
                                                                denier_change_starttime = ps_endtime
                                                                denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                                starttime = denier_change_endtime
                                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)


                                                self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                    self.machine_available_from[machine][2],self.machine_available_from[machine][5],
                                                                    row['TapeId'], row['Tape_name'],row['TapePropertyName'],row['Seq'], 
                                                                    row['TapeDenier'],row['Tape_Width'],row['TapeColour'],
                                                                    row['TapeMarking'],row['Demand_Source'],
                                                                    row['Total_Demand'],row['Weft_Demand'],row['Warp_Demand'],row['Warp_rf_Demand'],
                                                                    
                                                                    total_demand,row['Tape_Completion_date'],machine,row['Loom_Location'],row['LoomNo'],
                                                                    row['LoomType'],
                                                                    row['PreLogic_FinalFactor'],
                                                                    denier_change_starttime,denier_change_endtime,recipe_change_starttime,recipe_change_endtime,
                                                                    width_change_starttime,width_change_endtime,
                                                                    starttime,completion_time, f"Plan {self.machine_available_from[machine][4]}", 
                                                                    changeover_data[0],changeover_data[2],changeover_data[3],changeover_data[4]])
                                                

                                                                    
                                                self.machine_available_from[machine] = [tape_id, tape_name, tape_denier,completion_time, 
                                                    self.machine_available_from[machine][4] + 1,row['TapeColour'],
                                                    row['TapeUv'],row['TapeRP'],row['TapeFiller'],row['Tape_Width']]
                                    color = 2
                            elif(row['TapeColour']=='white'):
                                #white tapes of this machine           
                                prev_tape_id = self.machine_available_from[machine][0]
                                prev_tape_denier = self.machine_available_from[machine][2]
                                prev_tape_width = self.machine_available_from[machine][9]
                                tape_width = row['Tape_Width']
                                width_change_starttime = 0
                                width_change_endtime = 0
                                recipe_change_starttime = 0
                                recipe_change_endtime = 0
                                denier_change_starttime = 0
                                denier_change_endtime = 0
                                time_taken = 0
                                total_demand = 0
                                completion_time = 0
                                changeover_data = self.get_changeover_data(prev_tape_id, tape_id, machine)
                                width_changetime = self.get_width_changetime(prev_tape_width,tape_width)
                                recipe_changetime = self.get_recipe_changetime(machine,tape_UV,tape_Colour,tape_Filler)
                                
                                #width changeover
                                if width_changetime!=0 :
                                    width_change_starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                    width_change_endtime = width_change_starttime + pd.DateOffset(minutes=width_changetime)
                                    starttime = width_change_endtime #production start time
                                    total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                    time_taken = (total_demand) / row['Velocity']
                                    time_taken = round(time_taken * 60)
                                    completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                    
                                elif recipe_changetime !=0:
                                    recipe_change_starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))

                                    recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                    starttime = recipe_change_endtime #production start time
                                    total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                    time_taken = (total_demand) / row['Velocity']
                                    time_taken = round(time_taken * 60)
                                    completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                    if len(ps_starttime_values) > 0 and len(ps_endtime_values) > 0:
                                        ps_starttime = pd.Timestamp(ps_starttime_values[0])
                                        ps_endtime = pd.Timestamp(ps_endtime_values[0])
                                        if (recipe_change_starttime > ps_starttime) & (completion_time < ps_endtime):
                                                
                                            recipe_change_starttime = ps_endtime
                                            recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                            starttime = recipe_change_endtime
                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                        elif (recipe_change_starttime < ps_starttime) & (completion_time < ps_endtime)& (completion_time >ps_starttime):
                                                
                                            recipe_change_starttime = ps_endtime
                                            recipe_change_endtime = recipe_change_starttime+ pd.DateOffset(minutes=recipe_changetime)
                                            starttime = recipe_change_endtime
                                            completion_time = starttime + pd.DateOffset(hours=time_taken)
                                        elif (recipe_change_starttime < ps_starttime) & (completion_time > ps_endtime):
                                            
                                            
                                            recipe_change_starttime = ps_endtime
                                            recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                            starttime = recipe_change_endtime
                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                        elif (recipe_change_starttime > ps_starttime) & (completion_time > ps_endtime)&(recipe_change_starttime<ps_endtime):
                                            
                                            recipe_change_starttime = ps_endtime
                                            recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                            starttime = recipe_change_endtime
                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)

                                    
                                else:
                                    denier_changetime = self.get_denier_changetime_step(machine,tape_denier,row['TapeWidth'])
                                    if denier_changetime == 0:
                                        starttime =datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                        total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                        time_taken = (total_demand) / row['Velocity']
                                        time_taken = round(time_taken * 60)
                                        completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                    else:
                                        s = ((row['TapeDenier']-prev_tape_denier)/250) 
                                        if s<0 :
                                            t = abs(math.floor(s))
                                        else :
                                            t= abs(math.ceil(s))

                                        denier = prev_tape_denier
                                        tape_width = row['Tape_Width']
                                        
                                        st_time = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                        e_time = 0
                                        if t!=1:
                                            for i in range(1,t+1):
                                                name = 'Denier_Step'
                                                id = 0 
                                                demand=0
                                                #step up
                                                if (denier <=row['TapeDenier']):
                                                    if(s>0):
                                                        denier = denier + 250
                                                    den = self.regular_tapes['Tape_Denier'].values
                                                    filtered_rows = pd.DataFrame()
                                                    if (denier >row['TapeDenier']):
                                                        break
                                                    if (denier in den) :
                                                        filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == denier)
                                                                                        & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                        &(tape_width <= self.regular_tapes['Max Tape Width'])]                                                                                      
                                                        if not filtered_rows.empty:
                                                            name = filtered_rows['TapeName'].values[0]
                                                            id = filtered_rows['TapeId'].values[0]
                                                            demand = row['Velocity']*0.5

                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                            self.machine_available_from[machine][2],0,
                                                                            id,name,0,0, denier,0,0,0,0,
                                                                            demand,0,0,0,
                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                            row['LoomType'],
                                                                            0,
                                                                            0,0,0,0,0,0,
                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                            0,0,0,0])
                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                            st_time = e_time 
                                                            
                                                    elif denier not in den or filtered_rows.empty==True :
                                                        
                                                        denn = [x for x in den if (denier-250 <x )& (denier >x)]
                                                        closest_denier = min(denn , key=lambda x: abs(x - denier))
                                                        
                                                        if closest_denier < row['TapeDenier']:
                                                            filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == closest_denier )
                                                                                    & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                    &(tape_width <= self.regular_tapes['Max Tape Width'])]          
                                                        if not filtered_rows.empty:
                                                            name = filtered_rows['TapeName'].values[0]
                                                            id = filtered_rows['TapeId'].values[0]
                                                            demand = row['Velocity']*0.5
                                                            denier = closest_denier
                                                            print("Closest_denier : ",denier)

                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                            self.machine_available_from[machine][2],0,
                                                                            id,name,0,0, denier,0,0,0,0,
                                                                            demand,0,0,0,
                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                            row['LoomType'],
                                                                            0,
                                                                            0,0,0,0,0,0,
                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                            0,0,0,0])
                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                            st_time = e_time 
                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                            0,0,
                                                            0,0,0,0]
                                                            
                                                        else:
                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                            self.machine_available_from[machine][2],0,
                                                                            id,name,0,0, denier,0,0,0,0,
                                                                            demand,0,0,0,
                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                            row['LoomType'],
                                                                            0,
                                                                            st_time,st_time + pd.DateOffset(minutes=30),0,0,
                                                                            0,0,0,0, 0,
                                                                            0,0,0,0])                                    

                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                            st_time = e_time 
                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                            0,0,
                                                            0,0,0,0]
                                                #step down
                                                if (denier >=row['TapeDenier']) :           
                                                    if(s<0):
                                                        denier = denier - 250
                                                    den = self.regular_tapes['Tape_Denier'].values
                                                    filtered_rows = pd.DataFrame()
                                                    if (denier <row['TapeDenier']):
                                                        break
                                                    if (denier in den) :
                                                        filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == denier)
                                                                                        & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                        &(tape_width <= self.regular_tapes['Max Tape Width'])]                                                                                      
                                                        if not filtered_rows.empty:
                                                            name = filtered_rows['TapeName'].values[0]
                                                            id = filtered_rows['TapeId'].values[0]
                                                            demand = row['Velocity']*0.5

                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                            self.machine_available_from[machine][2],0,
                                                                            id,name,0,0, denier,0,0,0,0,
                                                                            demand,0,0,0,
                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                            row['LoomType'],
                                                                            0,
                                                                            0,0,0,0,0,0,
                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                            0,0,0,0])
                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                            st_time = e_time 
                                                            
                                                    elif denier not in den or filtered_rows.empty==True :
                                                        
                                                        denn = [x for x in den if (denier-250 <x )& (denier >x)]
                                                        closest_denier = min(denn , key=lambda x: abs(x - denier))
                                                        print("Closeest_denier1:",closest_denier)
                                                        if closest_denier >row['TapeDenier']:
                                                            filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == closest_denier )
                                                                                    & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                    &(tape_width <= self.regular_tapes['Max Tape Width'])]          
                                                        if not filtered_rows.empty:
                                                            name = filtered_rows['TapeName'].values[0]
                                                            id = filtered_rows['TapeId'].values[0]
                                                            demand = row['Velocity']*0.5
                                                            denier = closest_denier
                                                            print("Closest_denier : ",denier)

                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                            self.machine_available_from[machine][2],0,
                                                                            id,name,0,0, denier,0,0,0,0,
                                                                            demand,0,0,0,
                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                            row['LoomType'],
                                                                            0,
                                                                            0,0,0,0,
                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                            0,0,0,0])
                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                            st_time = e_time 
                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                            0,0,
                                                            0,0,0,0]
                                                            
                                                        else:
                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                            self.machine_available_from[machine][2],0,
                                                                            id,name,0,0, denier,0,0,0,0,
                                                                            demand,0,0,0,
                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                            row['LoomType'],
                                                                            0,
                                                                            st_time,st_time + pd.DateOffset(minutes=30),0,0,
                                                                            0,0,0, 0,0,
                                                                            0,0,0,0])                                    

                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                            st_time = e_time 
                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                            0,0,
                                                            0,0,0,0]
                                                        
                                        denier_change_starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))

                                        denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                        
                                        starttime = denier_change_endtime
                                        total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                        time_taken = (total_demand) / row['Velocity']
                                        time_taken = round(time_taken * 60)
                                        completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                        
                                        if len(ps_starttime_values) > 0 and len(ps_endtime_values) > 0:
                                            ps_starttime = pd.Timestamp(ps_starttime_values[0])
                                            ps_endtime = pd.Timestamp(ps_endtime_values[0])
                                            if (denier_change_starttime > ps_starttime) & (completion_time < ps_endtime):
                                                
                                                denier_change_starttime = ps_endtime
                                                denier_change_endtime = denier_change_starttime+ pd.DateOffset(minutes=denier_changetime)
                                                starttime = denier_change_endtime
                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                
                                            elif (denier_change_starttime < ps_starttime) & (completion_time < ps_endtime)& (completion_time >ps_starttime):
                                                
                                                denier_change_starttime = ps_endtime
                                                denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                starttime = denier_change_endtime
                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                            elif (denier_change_starttime < ps_starttime) & (completion_time > ps_endtime):
                                                
                                                denier_change_starttime = ps_endtime
                                                denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                starttime = denier_change_endtime
                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                            elif (denier_change_starttime > ps_starttime) & (completion_time > ps_endtime) & (denier_change_starttime<ps_endtime):
                                                
                                                denier_change_starttime = ps_endtime
                                                denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                starttime = denier_change_endtime
                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)
                        
                                #self.schedule = [['Prev_Tape_Id', 'Prev_Tape_name','Prev_Tape_Denier' ,'Tape_id', 'Tape_name','Tape_Denier' ,'Tape_demand', 'Tape_deamand + Production loss + Wastage','Target_date', 'Machine','Plant_Location','TapeDenier_Change_Starttime','TapeDenier_Change_Endtime','Changeover_Starttime','Changeover_Endtime','Prod_Start_Time', 'Prod_End_time', 'Plan no.', 'Type of Changeover', 'Changeover_time', 'Production_loss (kg)', 'Wastage (kg)', 'Man power loss (Rs)']]
                                #self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],self.machine_available_from[machine][2],self.machine_available_from[machine][-1],row['TapeId'], row['Tape_name'], row['TapeDenier'],row['TapeColour'],row['TapeMarking'],row['Tape_Demand'],total_demand,row['Tape_Completion_date'],machine,row['Loom_Location'],denier_change_starttime,denier_change_Endtime,changeover_starttime,changeover_endtime,starttime,completion_time, f"Plan {self.machine_available_from[machine][4]}", changeover_data[0],changeover_time,changeover_data[2],changeover_data[3],changeover_data[4]])
                                self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                        self.machine_available_from[machine][2],self.machine_available_from[machine][5],
                                                        row['TapeId'], row['Tape_name'],row['TapePropertyName'],row['Seq'], 
                                                        row['TapeDenier'],row['Tape_Width'],row['TapeColour'],row['TapeMarking'],
                                                        row['Demand_Source'],
                                                        row['Total_Demand'],row['Weft_Demand'],row['Warp_Demand'],row['Warp_rf_Demand'],
                                                        
                                                        total_demand,row['Tape_Completion_date'],machine,row['Loom_Location'],row['LoomNo'],
                                                        row['LoomType'],
                                                        row['PreLogic_FinalFactor'],
                                                        denier_change_starttime,denier_change_endtime,recipe_change_starttime,recipe_change_endtime,
                                                        width_change_starttime,width_change_endtime,
                                                        starttime,completion_time, f"Plan {self.machine_available_from[machine][4]}", 
                                                        changeover_data[0],changeover_data[2],changeover_data[3],changeover_data[4]])
                                
    
    
                                    
                                self.machine_available_from[machine] = [tape_id, tape_name, tape_denier,completion_time, 
                                                                        self.machine_available_from[machine][4] + 1,
                                                                        row['TapeColour'],row['TapeFiller'],row['TapeRP'],row['TapeUv'],row['Tape_Width']]

                else:
                    filtered_schedule = self.schedule_df.loc[(self.schedule_df['BestMachine'] == machine)]
                    
                    unique_completion = filtered_schedule['Tape_Completion_date'].unique()
                    
                    for completion in unique_completion: 
                        filtered_schedule1 =  filtered_schedule.loc[filtered_schedule['Tape_Completion_date']==completion]
                        
                        unique_denier = filtered_schedule1['TapeDenier'].unique() 
                        desired_order = self.find_nearest_denier(self.machine_available_from[machine][2],list(unique_denier))
                        filtered_schedule2 = filtered_schedule1.sort_values(by='TapeDenier', key=lambda x: x.map(dict(zip(desired_order, range(len(desired_order))))))
                        for index,row in filtered_schedule2.iterrows():  
                            tape_id = row['TapeId']
                            tape_name = row['Tape_name']
                            tape_denier = row['TapeDenier']
                            tape_UV = row['TapeUv']
                            tape_Colour = row['TapeColour']
                            tape_Filler = row['TapeFiller']
                            
                            #color tapes of this machine
                            if(row['TapeColour'] != 'white'): 
                                if (color==2):
                                    continue
                                else :              

                                    filtered_schedule3 = filtered_schedule.loc[(filtered_schedule['TapeColour'] != 'white')]
                                    
                                    unique_color = filtered_schedule3['TapeColour'].unique() 
                                    
                                    for colour in unique_color: 
                                        
                                        filtered_schedule4 =  filtered_schedule3.loc[filtered_schedule3['TapeColour']==colour]
                                        unique_completion = filtered_schedule4['Tape_Completion_date'].unique()
                                        for completion in unique_completion: 
                                            filtered_schedule5 =  filtered_schedule4.loc[filtered_schedule4['Tape_Completion_date']==completion]
                                            unique_denier = filtered_schedule5['TapeDenier'].unique()
                                            desired_order = self.find_nearest_denier(self.machine_available_from[machine][2],list(unique_denier))
                                            filtered_schedule6 = filtered_schedule5.sort_values(by='TapeDenier', key=lambda x: x.map(dict(zip(desired_order, range(len(desired_order))))))
                                            
                                            for index,row in filtered_schedule6.iterrows():  
                                                tape_id = row['TapeId']
                                                tape_name = row['Tape_name']
                                                tape_denier = row['TapeDenier']
                                                tape_UV = row['TapeUv']
                                                tape_Colour = row['TapeColour']
                                                tape_Filler = row['TapeFiller']
                                                tape_width = row['Tape_Width']
                                                
                                                prev_tape_id = self.machine_available_from[machine][0]
                                                prev_tape_denier = self.machine_available_from[machine][2]
                                                prev_tape_width = self.machine_available_from[machine][9]
                                                
                                                width_change_starttime = 0
                                                width_change_endtime = 0
                                                recipe_change_starttime = 0
                                                recipe_change_endtime = 0
                                                denier_change_starttime = 0
                                                denier_change_endtime = 0
                                                time_taken = 0
                                                total_demand = 0
                                                completion_time = 0
                                                changeover_data = self.get_changeover_data(prev_tape_id, tape_id, machine)
                                                width_changetime = self.get_width_changetime(prev_tape_width,tape_width)
                                                
                                                recipe_changetime = self.get_recipe_changetime(machine,tape_UV,tape_Colour,tape_Filler)
                                                
                                                if width_changetime!=0:
                                                    width_change_starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                                    width_change_endtime = width_change_starttime + pd.DateOffset(minutes=width_changetime)
                                                    starttime = width_change_endtime #production start time
                                                    total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                                    time_taken = (total_demand) / row['Velocity']
                                                    time_taken = round(time_taken * 60)
                                                    completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                
                                                    
                                                elif recipe_changetime !=0:
                                                    
                                                    recipe_change_starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))

                                                    recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                                    starttime = recipe_change_endtime #production start time
                                                    total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                                    time_taken = (total_demand) / row['Velocity']
                                                    time_taken = round(time_taken * 60)
                                                    completion_time = starttime + pd.DateOffset(minutes=time_taken)

                                                    if len(ps_starttime_values) > 0 and len(ps_endtime_values) > 0:
                                                        ps_starttime = pd.Timestamp(ps_starttime_values[0])
                                                        ps_endtime = pd.Timestamp(ps_endtime_values[0])
                                                        if (recipe_change_starttime > ps_starttime) & (completion_time < ps_endtime):
                                                                
                                                            recipe_change_starttime = ps_endtime
                                                            recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                                            starttime = recipe_change_endtime
                                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                        elif (recipe_change_starttime < ps_starttime) & (completion_time < ps_endtime) & (completion_time >ps_starttime):
                                                                
                                                            recipe_change_starttime = ps_endtime
                                                            recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                                            starttime = recipe_change_endtime
                                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                        elif (recipe_change_starttime < ps_starttime) & (completion_time > ps_endtime):

                                                                
                                                            recipe_change_starttime = ps_endtime
                                                            recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                                            starttime = recipe_change_endtime
                                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                        elif (recipe_change_starttime > ps_starttime) & (completion_time > ps_endtime)&(recipe_change_starttime<ps_endtime):

                                                                
                                                            recipe_change_starttime = ps_endtime
                                                            recipe_change_endtime = recipe_change_starttime+ pd.DateOffset(minutes=recipe_changetime)
                                                            starttime = recipe_change_endtime
                                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                else:
                                                    denier_changetime = self.get_denier_changetime_step(machine,tape_denier,row['TapeWidth'])
                                                    if denier_changetime == 0 :
                                                        starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                                        total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                                        time_taken = (total_demand) / row['Velocity']
                                                        time_taken = round(time_taken * 60)
                                                        completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                    else:
                                                        s = ((row['TapeDenier']-prev_tape_denier)/250) 
                                                        if s<0 :
                                                            t = abs(math.floor(s))
                                                        else :
                                                            t= abs(math.ceil(s))

                                                        denier = prev_tape_denier
                                                        tape_width = row['Tape_Width']
                                                        
                                                        st_time = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                                        e_time = 0
                                                        if t!=1:
                                                            for i in range(1,t+1):
                                                                name = 'Denier_Step'
                                                                id = 0 
                                                                demand=0
                                                                #step up
                                                                if (denier <=row['TapeDenier']):
                                                                    if(s>0):
                                                                        denier = denier + 250
                                                                    den = self.regular_tapes['Tape_Denier'].values
                                                                    filtered_rows = pd.DataFrame()
                                                                    if (denier >row['TapeDenier']):
                                                                        break
                                                                    if (denier in den) :
                                                                        filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == denier)
                                                                                                        & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                                        &(tape_width <= self.regular_tapes['Max Tape Width'])]                                                                                      
                                                                        if not filtered_rows.empty:
                                                                            name = filtered_rows['TapeName'].values[0]
                                                                            id = filtered_rows['TapeId'].values[0]
                                                                            demand = row['Velocity']*0.5
                    
                                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                            self.machine_available_from[machine][2],0,
                                                                                            id,name,0,0, denier,0,0,0,0,
                                                                                            demand,0,0,0,
                                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                                            row['LoomType'],
                                                                                            0,
                                                                                            0,0,0,0,0,0,
                                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                                            0,0,0,0])
                                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                                            st_time = e_time 
                                                                            
                                                                    elif denier not in den or filtered_rows.empty==True :
                                                                        
                                                                        denn = [x for x in den if (denier-250 <x )& (denier >x)]
                                                                        closest_denier = min(denn , key=lambda x: abs(x - denier))
                                                                        print("Closeest_denier1:",closest_denier)
                                                                        if closest_denier < row['TapeDenier']:
                                                                            filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == closest_denier )
                                                                                                    & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                                    &(tape_width <= self.regular_tapes['Max Tape Width'])]          
                                                                        if not filtered_rows.empty:
                                                                            name = filtered_rows['TapeName'].values[0]
                                                                            id = filtered_rows['TapeId'].values[0]
                                                                            demand = row['Velocity']*0.5
                                                                            denier = closest_denier
                                                                            print("Closest_denier : ",denier)
                    
                                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                            self.machine_available_from[machine][2],0,
                                                                                            id,name,0,0, denier,0,0,0,0,
                                                                                            demand,0,0,0,
                                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                                            row['LoomType'],
                                                                                            0,
                                                                                            0,0,0,0,0,0,
                                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                                            0,0,0,0])
                                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                                            st_time = e_time 
                                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                                            0,0,
                                                                            0,0,0,0]
                                                                            
                                                                        else:
                                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                            self.machine_available_from[machine][2],0,
                                                                                            id,name,0,0, denier,0,0,0,0,
                                                                                            demand,0,0,0,
                                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                                            row['LoomType'],
                                                                                            0,
                                                                                            st_time,st_time + pd.DateOffset(minutes=30),0,0,
                                                                                            0,0,0, 0,0,
                                                                                            0,0,0,0])                                    

                                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                                            st_time = e_time 
                                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                                            0,0,
                                                                            0,0,0,0]
                                                                #step down
                                                                if (denier >=row['TapeDenier']):            
                                                                    if(s<0):
                                                                        denier = denier - 250
                                                                    den = self.regular_tapes['Tape_Denier'].values
                                                                    filtered_rows = pd.DataFrame()
                                                                    if (denier <row['TapeDenier']):
                                                                        break
                                                                    if (denier in den) :
                                                                        filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == denier)
                                                                                                        & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                                        &(tape_width <= self.regular_tapes['Max Tape Width'])]                                                                                      
                                                                        if not filtered_rows.empty:
                                                                            name = filtered_rows['TapeName'].values[0]
                                                                            id = filtered_rows['TapeId'].values[0]
                                                                            demand = row['Velocity']*0.5
                    
                                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                            self.machine_available_from[machine][2],0,
                                                                                            id,name,0,0, denier,0,0,0,0,
                                                                                            demand,0,0,0,
                                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                                            row['LoomType'],
                                                                                            0,
                                                                                            0,0,0,0,0,0,
                                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                                            0,0,0,0])
                                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                                            st_time = e_time 
                                                                            
                                                                    elif denier not in den or filtered_rows.empty==True :
                                                                        
                                                                        denn = [x for x in den if (denier-250 <x )& (denier >x)]
                                                                        closest_denier = min(denn , key=lambda x: abs(x - denier))
                                                                        
                                                                        if closest_denier >row['TapeDenier']:
                                                                            filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == closest_denier )
                                                                                                    & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                                    &(tape_width <= self.regular_tapes['Max Tape Width'])]          
                                                                        if not filtered_rows.empty:
                                                                            name = filtered_rows['TapeName'].values[0]
                                                                            id = filtered_rows['TapeId'].values[0]
                                                                            demand = row['Velocity']*0.5
                                                                            denier = closest_denier
                                                                            
                    
                                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                            self.machine_available_from[machine][2],0,
                                                                                            id,name,0,0, denier,0,0,0,0,
                                                                                            demand,0,0,0,
                                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                                            row['LoomType'],
                                                                                            0,
                                                                                            0,0,0,0,0,0,
                                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                                            0,0,0,0])
                                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                                            st_time = e_time 
                                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                                            0,0,
                                                                            0,0,0,0]
                                                                            
                                                                        else:
                                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                            self.machine_available_from[machine][2],0,
                                                                                            id,name,0,0, denier,0,0,0,0,
                                                                                            demand,0,0,0,
                                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                                            row['LoomType'],
                                                                                            0,
                                                                                            st_time,st_time + pd.DateOffset(minutes=30),0,0,
                                                                                            0,0,0, 0,0,
                                                                                            0,0,0,0])                                    

                                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                                            st_time = e_time 
                                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                                            0,0,
                                                                            0,0,0,0]
                                                        denier_change_starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))

                                                        denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                        starttime = denier_change_endtime
                                                        total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                                        time_taken = (total_demand) / row['Velocity']
                                                        time_taken = round(time_taken * 60)
                                                        completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                        if len(ps_starttime_values) > 0 and len(ps_endtime_values) > 0:
                                                            ps_starttime = pd.Timestamp(ps_starttime_values[0])
                                                            ps_endtime = pd.Timestamp(ps_endtime_values[0])
                                                            if (denier_change_starttime > ps_starttime) & (completion_time < ps_endtime):
                                                                    
                                                                denier_change_starttime = ps_endtime
                                                                
                                                                denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                                starttime = denier_change_endtime
                                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                                
                                                            elif (denier_change_starttime < ps_starttime) & (completion_time < ps_endtime)& (completion_time >ps_starttime):
                                                                    
                                                                denier_change_starttime = ps_endtime
                                                                denier_change_endtime =  denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                                starttime = denier_change_endtime
                                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                            elif (denier_change_starttime < ps_starttime) & (completion_time > ps_endtime):
                                                                    
                                                                denier_change_starttime = ps_endtime
                                                                
                                                                denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                                starttime = denier_change_endtime
                                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                            elif (denier_change_starttime > ps_starttime) & (completion_time > ps_endtime)& (denier_change_starttime<ps_endtime):
                                                                    
                                                                denier_change_starttime = ps_endtime
                                                                denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                                starttime = denier_change_endtime
                                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)

                                                                    
                                    
    
                                                self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                                    self.machine_available_from[machine][2],self.machine_available_from[machine][5],
                                                                    row['TapeId'], row['Tape_name'],row['TapePropertyName'],row['Seq'],
                                                                    row['TapeDenier'],row['Tape_Width'],row['TapeColour'],row['TapeMarking'],
                                                                    row['Demand_Source'],
                                                                    row['Total_Demand'],row['Weft_Demand'],row['Warp_Demand'],row['Warp_rf_Demand'],
                                                                    
                                                                    total_demand,row['Tape_Completion_date'],machine,row['Loom_Location'],row['LoomNo'],
                                                                    row['LoomType'],
                                                                    row['PreLogic_FinalFactor'],
                                                                    denier_change_starttime,denier_change_endtime,recipe_change_starttime,recipe_change_endtime,
                                                                    width_change_starttime,width_change_endtime,
                                                                    starttime,completion_time, f"Plan {self.machine_available_from[machine][4]}", 
                                                                    changeover_data[0],changeover_data[2],changeover_data[3],changeover_data[4]])

    
                                                self.machine_available_from[machine] = [tape_id, tape_name, tape_denier,completion_time, 
                                                                                        self.machine_available_from[machine][4] + 1,row['TapeColour'],
                                                                                        row['TapeFiller'],row['TapeRP'],
                                                                                        row['TapeUv'],row['Tape_Width']]
                                    color = 2 
                            elif (row['TapeColour']=='white'):
                                #white tapes of this machine
                                                
                                prev_tape_id = self.machine_available_from[machine][0]
                                prev_tape_denier = self.machine_available_from[machine][2]
                                prev_tape_width = self.machine_available_from[machine][9]
                                
                                tape_width = row['Tape_Width']
                                
                                
                                
                                width_change_starttime =0 
                                width_change_endtime = 0
                                recipe_change_starttime = 0
                                recipe_change_endtime = 0
                                denier_change_starttime = 0
                                denier_change_endtime = 0
                                time_taken = 0
                                total_demand = 0
                                completion_time = 0
                                changeover_data = self.get_changeover_data(prev_tape_id, tape_id, machine)
                                width_changetime = self.get_width_changetime(prev_tape_width,tape_width)
                                recipe_changetime = self.get_recipe_changetime(machine,tape_UV,tape_Colour,tape_Filler)
                                
                                if width_changetime!=0:
                                    width_change_starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                    width_change_endtime = width_change_starttime + pd.DateOffset(minutes=width_changetime)
                                    starttime = width_change_endtime #production start time
                                    total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                    time_taken = (total_demand) / row['Velocity']
                                    time_taken = round(time_taken * 60)
                                    completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                    
                                elif recipe_changetime !=0:
                                    recipe_change_starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))

                                    recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                    starttime = recipe_change_endtime #production start time
                                    total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                    time_taken = (total_demand) / row['Velocity']
                                    time_taken = round(time_taken * 60)
                                    completion_time = starttime + pd.DateOffset(minutes=time_taken)

                                    if len(ps_starttime_values) > 0 and len(ps_endtime_values) > 0:
                                        ps_starttime = pd.Timestamp(ps_starttime_values[0])
                                        ps_endtime = pd.Timestamp(ps_endtime_values[0])
                                        if (recipe_change_starttime > ps_starttime) & (completion_time < ps_endtime):
                                                
                                            recipe_change_starttime = ps_endtime
                                            recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                            starttime = recipe_change_endtime
                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                        elif (recipe_change_starttime < ps_starttime) & (completion_time < ps_endtime)& (completion_time >ps_starttime):
                                                
                                            recipe_change_starttime = ps_endtime
                                            recipe_change_endtime = recipe_change_starttime+ pd.DateOffset(minutes=recipe_changetime)
                                            starttime = recipe_change_endtime
                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                        elif (recipe_change_starttime < ps_starttime) & (completion_time > ps_endtime):
                                            
                                            
                                            recipe_change_starttime = ps_endtime
                                            recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                            starttime = recipe_change_endtime
                                            
                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                        elif (recipe_change_starttime > ps_starttime) & (completion_time > ps_endtime)&(recipe_change_starttime<ps_endtime):
                                            
                                            recipe_change_starttime = ps_endtime
                                            recipe_change_endtime = recipe_change_starttime + pd.DateOffset(minutes=recipe_changetime)
                                            starttime = recipe_change_endtime
                                            completion_time = starttime + pd.DateOffset(minutes=time_taken)
        
                                else:
                                    denier_changetime = self.get_denier_changetime_step(machine,tape_denier,row['TapeWidth'])
                                    if denier_changetime == 0:
                                        starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                        total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                        time_taken = (total_demand) / row['Velocity']
                                        time_taken = round(time_taken * 60)
                                        completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                    else:
                                        s = ((row['TapeDenier']-prev_tape_denier)/250) 
                                        if s<0 :
                                            t = abs(math.floor(s))
                                        else :
                                            t= abs(math.ceil(s))

                                        denier = prev_tape_denier
                                        tape_width = row['Tape_Width']
                                        
                                        st_time = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))
                                        e_time = 0
                                        if t!=1:
                                            for i in range(1,t+1):
                                                name = 'Denier_Step'
                                                id = 0 
                                                demand=0
                                                #step up
                                                if (denier <=row['TapeDenier']):
                                                    if(s>0):
                                                        denier = denier + 250
                                                    den = self.regular_tapes['Tape_Denier'].values
                                                    filtered_rows = pd.DataFrame()
                                                    if (denier >row['TapeDenier']):
                                                        break
                                                    if (denier in den) :
                                                        filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == denier)
                                                                                        & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                        &(tape_width <= self.regular_tapes['Max Tape Width'])]                                                                                      
                                                        if not filtered_rows.empty:
                                                            name = filtered_rows['TapeName'].values[0]
                                                            id = filtered_rows['TapeId'].values[0]
                                                            demand = row['Velocity']*0.5

                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                            self.machine_available_from[machine][2],0,
                                                                            id,name,0,0, denier,0,0,0,0,
                                                                            demand,0,0,0,
                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                            row['LoomType'],
                                                                            0,
                                                                            0,0,0,0,0,0,
                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                            0,0,0,0])
                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                            st_time = e_time 
                                                            
                                                    elif denier not in den or filtered_rows.empty==True :
                                                        
                                                        denn = [x for x in den if (denier-250 <x )& (denier >x)]
                                                        closest_denier = min(denn , key=lambda x: abs(x - denier))
                                                        
                                                        if closest_denier < row['TapeDenier']:
                                                            filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == closest_denier )
                                                                                    & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                    &(tape_width <= self.regular_tapes['Max Tape Width'])]          
                                                        if not filtered_rows.empty:
                                                            name = filtered_rows['TapeName'].values[0]
                                                            id = filtered_rows['TapeId'].values[0]
                                                            demand = row['Velocity']*0.5
                                                            denier = closest_denier
                                                            print("Closest_denier : ",denier)

                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                            self.machine_available_from[machine][2],0,
                                                                            id,name,0,0, denier,0,0,0,0,
                                                                            demand,0,0,0,
                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                            row['LoomType'],
                                                                            0,
                                                                            0,0,0,0,0,0,
                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                            0,0,0,0])
                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                            st_time = e_time 
                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                            0,0,
                                                            0,0,0,0,0]
                                                            
                                                        else:
                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                            self.machine_available_from[machine][2],0,
                                                                            id,name,0,0, denier,0,0,0,0,
                                                                            demand,0,0,0,
                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                            row['LoomType'],
                                                                            0,
                                                                            st_time,st_time + pd.DateOffset(minutes=30),0,0,
                                                                            0,0,0, 0,0,
                                                                            0,0,0,0])                                    

                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                            st_time = e_time 
                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                            0,0,
                                                            0,0,0,0]
                                                #step down
                                                if (denier >=row['TapeDenier']) :            
                                                    if(s<0):
                                                        denier = denier - 250
                                                    den = self.regular_tapes['Tape_Denier'].values
                                                    filtered_rows = pd.DataFrame()
                                                    if (denier <row['TapeDenier']):
                                                        break
                                                    if (denier in den) :
                                                        filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == denier)
                                                                                        & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                        &(tape_width <= self.regular_tapes['Max Tape Width'])]                                                                                      
                                                        if not filtered_rows.empty:
                                                            name = filtered_rows['TapeName'].values[0]
                                                            id = filtered_rows['TapeId'].values[0]
                                                            demand = row['Velocity']*0.5

                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                            self.machine_available_from[machine][2],0,
                                                                            id,name,0,0, denier,0,0,0,0,
                                                                            demand,0,0,0,
                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                            row['LoomType'],
                                                                            0,
                                                                            0,0,0,0,0,0,
                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                            0,0,0,0])
                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                            st_time = e_time 
                                                            
                                                    elif denier not in den or filtered_rows.empty==True :
                                                        
                                                        denn = [x for x in den if (denier-250 <x )& (denier >x)]
                                                        closest_denier = min(denn , key=lambda x: abs(x - denier))
                                                        print("Closeest_denier1:",closest_denier)
                                                        if closest_denier >row['TapeDenier']:
                                                            filtered_rows = self.regular_tapes[(self.regular_tapes['Tape_Denier'] == closest_denier )
                                                                                    & (tape_width >= self.regular_tapes['Min Tape Width'])
                                                                                    &(tape_width <= self.regular_tapes['Max Tape Width'])]          
                                                        if not filtered_rows.empty:
                                                            name = filtered_rows['TapeName'].values[0]
                                                            id = filtered_rows['TapeId'].values[0]
                                                            demand = row['Velocity']*0.5
                                                            denier = closest_denier
                                                            print("Closest_denier : ",denier)

                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                            self.machine_available_from[machine][2],0,
                                                                            id,name,0,0, denier,0,0,0,0,
                                                                            demand,0,0,0,
                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                            row['LoomType'],
                                                                            0,
                                                                            0,0,0,0,0,0,
                                                                            st_time,st_time + pd.DateOffset(minutes=30), 0, 
                                                                            0,0,0,0])
                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                            st_time = e_time 
                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                            0,0,
                                                            0,0,0,0]
                                                            
                                                        else:
                                                            self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                            self.machine_available_from[machine][2],0,
                                                                            id,name,0,0, denier,0,0,0,0,
                                                                            demand,0,0,0,
                                                                            0,0,machine,row['Loom_Location'],row['LoomNo'],
                                                                            row['LoomType'],
                                                                            0,
                                                                            st_time,st_time + pd.DateOffset(minutes=30),0,0,
                                                                            0,0,0,0,0,
                                                                            0,0,0,0])                                    

                                                            e_time = st_time + pd.DateOffset(minutes=30)
                                                            st_time = e_time 
                                                            self.machine_available_from[machine] = [id, name,denier,e_time, 
                                                            0,0,
                                                            0,0,0,0]
                                        denier_change_starttime = datetime.strptime(str(self.machine_available_from[machine][3]), '%Y-%m-%d %H:%M'+str(":00"))

                                        denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                        
                                        starttime = denier_change_endtime
                                        total_demand = row['Total_Demand'] +changeover_data[2]+changeover_data[3]
                                        time_taken = (total_demand) / row['Velocity']
                                        time_taken = round(time_taken * 60)
                                        completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                        
                                        if len(ps_starttime_values) > 0 and len(ps_endtime_values) > 0:
                                            ps_starttime = pd.Timestamp(ps_starttime_values[0])
                                            ps_endtime = pd.Timestamp(ps_endtime_values[0])
                                            if (denier_change_starttime > ps_starttime) & (completion_time < ps_endtime):
                                                
                                                denier_change_starttime = ps_endtime
                                                denier_change_endtime = denier_change_starttime+ pd.DateOffset(minutes=denier_changetime)
                                                starttime = denier_change_endtime
                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                                
                                            elif (denier_change_starttime < ps_starttime) & (completion_time < ps_endtime)& (completion_time >ps_starttime):
                                                
                                                denier_change_starttime = ps_endtime
                                                denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                starttime = denier_change_endtime
                                                completion_time = starttime + pd.DateOffset(minutes=time_taken)
                                            elif (denier_change_starttime < ps_starttime) & (completion_time > ps_endtime):
                                                
                                                denier_change_starttime = ps_endtime
                                                denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                starttime = denier_change_endtime
                                                completion_time = starttime+ pd.DateOffset(minutes=time_taken)
                                            elif (denier_change_starttime > ps_starttime) & (completion_time > ps_endtime) & (denier_change_starttime<ps_endtime):
                                                
                                                denier_change_starttime = ps_endtime
                                                denier_change_endtime = denier_change_starttime + pd.DateOffset(minutes=denier_changetime)
                                                starttime = denier_change_endtime
                                                completion_time =starttime + pd.DateOffset(minutes=time_taken)
    
                                self.schedule.append([self.machine_available_from[machine][0], self.machine_available_from[machine][1],
                                                        self.machine_available_from[machine][2],self.machine_available_from[machine][5],
                                                        row['TapeId'], row['Tape_name'],row['TapePropertyName'],row['Seq'], 
                                                        row['TapeDenier'],row['Tape_Width'],row['TapeColour'],row['TapeMarking'],
                                                        row['Demand_Source'],
                                                        row['Total_Demand'],row['Weft_Demand'],row['Warp_Demand'],row['Warp_rf_Demand'],
                                                        
                                                        total_demand,row['Tape_Completion_date'],machine,row['Loom_Location'],row['LoomNo'],
                                                        row['LoomType'],
                                                        row['PreLogic_FinalFactor'],
                                                        denier_change_starttime,denier_change_endtime,recipe_change_starttime,recipe_change_endtime,
                                                        width_change_starttime,width_change_endtime,
                                                        starttime,completion_time, f"Plan {self.machine_available_from[machine][4]}", 
                                                        changeover_data[0],changeover_data[2],changeover_data[3],changeover_data[4]])
                    
                                self.machine_available_from[machine] = [tape_id, tape_name, tape_denier,completion_time, 
                                                                        self.machine_available_from[machine][4] + 1,
                                                                        row['TapeColour'],row['TapeFiller'],row['TapeRP'],row['TapeUv'],row['Tape_Width']]
                            
            return [self.schedule]
        except Exception as e :
            print(e)
            self.Logs.Logging(f"File :generate_schedule, Function :  tape_scheduler_schedule_tapes, Status: not executed, Reason : {e}")
    
    def upload_schedule_to_DB(self, schedule_df, non_schedule_df):
        try:
            # Start a transaction
            with self.conx.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION;")
                cursor.execute("Truncate table Algo8_Tapeline_Schedule")
                cursor.execute("Truncate table Algo8_Tapeline_Not_Schedule")
                
                # Commit the transaction
                self.conx.commit()
                self.Logs.Logging("File: schedule, Function: upload_schedule_to_DB, Status: Tables are successfully Truncated")
        except Exception as e:
            # Rollback in case of error
            self.conx.rollback()
            self.Logs.Logging(f"File: schedule, Function: upload_schedule_to_DB, Status: Tables are Not Truncated, Reason: {e}")
            raise Exception("Not able to Truncate the tables") from e

        # Upload schedule to DB
        try:
            schedule_df["AuditDateTime"] = datetime.now()
            schedule_df.to_sql(name='Algo8_Tapeline_Schedule', con=self.engine, if_exists='append', index=False)
            print("Schedule data uploaded successfully!")
            self.Logs.Logging("File: Schedule, Function: upload_schedule_to_DB, Status: Schedule data added into DB")
        except Exception as e:
            print("Schedule data not uploaded!")
            self.Logs.Logging(f"File: Schedule, Function: upload_schedule_to_DB, Status: Schedule data Not added into DB, Reason: {e}")
            print(e)

        # Upload not_schedule to DB
        try:
            non_schedule_df["AuditDateTime"] = datetime.now()
            non_schedule_df.to_sql(name='Algo8_Tapeline_Not_Schedule', con=self.engine, if_exists='append', index=False)
            print("Non-schedule data uploaded successfully!")
            self.Logs.Logging("File: Schedule, Function: upload_schedule_to_DB, Status: Non-schedule data added into DB")
        except Exception as e:
            print("Non-schedule data not uploaded!")
            self.Logs.Logging(f"File: Schedule, Function: upload_schedule_to_DB, Status: Non-schedule data Not added into DB, Reason: {e}")
            print(e)
    
    def upload_schedule(self,schedule_df,non_schedule_df):
        try:
            cursor = self.conx.cursor();
            cursor.execute("Truncate table Algo8_Tapeline_Schedule")
            cursor.execute("Truncate table Algo8_Tapeline_Not_Schedule")
            self.conx.commit()
            cursor.close()
            print("Tables are Truncated!")
            self.Logs.Logging('File: Schedule, Function : upload schedule, Status: Tables are truncated successfully')
        except Exception as e:
            self.Logs.Logging(f"File: Schedule, Function: upload schedule, Status: Tabled are not Truncated, Reason {e}")
            raise "Not able to truncate the tables"
        
        try : 
            schedule_df["AuditDateTime"] = datetime.now()
            schedule_df.to_sql(name='Algo8_Tapeline_Schedule', con=self.engine, if_exists='append', index=False)
            print("Schedule data uploaded successfully!")
            self.Logs.Logging("File: Schedule, Function: upload_schedule_to_DB, Status: Schedule data added into DB")
        except Exception as e:
            print("Schedule data not uploaded!")
            self.Logs.Logging(f"File: Schedule, Function: upload_schedule_to_DB, Status: Schedule data Not added into DB, Reason: {e}")
            print(e)
            
        try : 
            non_schedule_df["AuditDateTime"] = datetime.now()
            non_schedule_df.to_sql(name ='Algo8_Tapeline_Not_Schedule',con = self.engine,if_exists = 'append',index=False)
            print("Not Schedule data uploaded successfully!")
            self.Logs.Logging("File: Schedule, Function: upload_schedule_to_DB, Status: non_Schedule data added into DB")
        except Exception as e:
            print("Not_Schedule data not uploaded!")
            self.Logs.Logging(f"File: Schedule, Function: upload_schedule_to_DB, Status: non_Schedule data Not added into DB, Reason: {e}")
            print(e)
            
            
def get_schedule():
    conn = Conn()
    conn.connect()
    engine = conn.get_engine()
    conx = conn.get_conx()
    production = Production(engine,conx)
    prod_data = production.read_production_data()
    prod_final = production.process_production()
    
    # Instantiate the ProductionAnalyzer class and pass prod_df as an argument
    analyzer = ProductionAnalyzer(prod_final)

    analyzer.calculate_velocity()
    
    tape_prod  = prod_final.copy()
    last_production = analyzer.last_production(tape_prod)



    scheduler = Bestmachines(last_production,analyzer,engine)
    
    schedule= scheduler.schedule_tapes(analyzer)
    
    schedule_df = pd.DataFrame(schedule[0][1:], columns=schedule[0][0])
    non_schedule_df = pd.DataFrame(schedule[1][1:], columns=schedule[1][0])
    
    scheduler = TapeScheduler(schedule_df,last_production,engine)
    schedule= scheduler.schedule_tapes(last_production)
    
    schedule_dff = pd.DataFrame(schedule[0][1:], columns=schedule[0][0])
    

    schedule_dff.to_excel('C:/Users/ATISHAY/Desktop/KPL_Tapeline/SCHEDULE.xlsx',index = False)
    non_schedule_df.to_excel('C:/Users/ATISHAY/Desktop/KPL_Tapeline/NON-SCHEDULE.xlsx',index = False)
    
    final_schedule = pd.read_excel('C:/Users/ATISHAY/Desktop/KPL_Tapeline/SCHEDULE.xlsx',index_col=False)
    final_non_schedule = pd.read_excel('C:/Users/ATISHAY/Desktop/KPL_Tapeline/NON-SCHEDULE.xlsx',index_col=False)
    
    final_schedule = pd.DataFrame(final_schedule)
    final_non_schedule = pd.DataFrame(final_non_schedule)
    
    scheduler.upload_schedule(final_schedule,final_non_schedule)
    
    print("schedule generated!!")
    return 'Success'

