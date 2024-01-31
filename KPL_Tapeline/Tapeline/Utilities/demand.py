import pandas as pd
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import warnings
import datetime 
warnings.filterwarnings('ignore')
#http://localhost:5001/app/v1/kpl/tapeline/calculate_demand
from ..Setups.Database.connection import Conn
from ..Setups.Loggers.Logger import Logs
import re


class TapeDemandCalculator:

    def __init__(self, engine,conx):
        '''In this init function first we will call Conn class and conncet to DB and then will store require information as self. .....'''
        
        self.engine = engine
        self.conx = conx
        self.Logs = Logs()
        
        self.fab_recp = pd.DataFrame()
        self.df_demand = pd.DataFrame()
        self.df_1_Master = pd.DataFrame()
        self.df_layers = pd.DataFrame()
        self.loom_location = pd.DataFrame()
        self.TapeDenier = pd.DataFrame()
        self.confg_days = pd.DataFrame()
        self.df_layers = pd.DataFrame()
        self.buffer_demand =pd.DataFrame()
        self.dfk = pd.DataFrame()
        self.dfkk = pd.DataFrame()
        self.dfkkk = pd.DataFrame()
        self.df_RP= pd.DataFrame()
        self.df_filler = pd.DataFrame()
        self.df_invent_D19 = pd.DataFrame()
        self.df_invent_GR = pd.DataFrame()
        
    def read_fabric_recipe(self):
        try :
            query = "SELECT * FROM [Algo8].[dbo].[FabricRecipe]"
            self.fab_recp = pd.read_sql(query, self.engine)
            
            df_warp = self.fab_recp[self.fab_recp['TapePropertyName'] == 'Warp'][['FabricId', 'TapeId', 'TapeName','NoOfTape_Auto','Master_Tape2Fabric_TapePercentage']]
            df_warp.columns = ['FabricId', 'warp_id', 'warp_name','warp_no_of_tape_auto','Warp%']
        
            # Filter Weft data
            df_weft = self.fab_recp[self.fab_recp['TapePropertyName'] == 'Weft'][['FabricId', 'TapeId', 'TapeName','NoOfTape_Auto','Master_Tape2Fabric_TapePercentage']]
            df_weft.columns = ['FabricId', 'weft_id', 'weft_name','weft_no_of_tape_auto','Weft%']
            
            df_warp_rf = self.fab_recp[self.fab_recp['TapePropertyName'] == 'Warp R/F'][['FabricId', 'TapeId', 'TapeName','NoOfTape_Auto','Master_Tape2Fabric_TapePercentage']]
            df_warp_rf.columns = ['FabricId', 'warp_rf_id',  'warp_rf_name','warp_rf_no_of_tape_auto','Warp_rf%']
        
            # Merge on FabricId
            df_ww = pd.merge(df_weft, df_warp, on='FabricId', how='left')
            df_ww = pd.merge(df_ww, df_warp_rf, on='FabricId', how='left')
            self.Logs.Logging("File: Demand, Function: read_fabric_recipe, Status: completed")
        except Exception as e:
            print(e)
            self.Logs.Logging(f"File: Demand, Function: read_fabric_recipe, Status: not completed  Reason : {e}")
            
        return df_ww
    
    def read_weaving_planning(self):
        try :
            query = "SELECT [Previous_FabricId],[Previous_FabricCode],[FabricId], [FabricCode], [ULFabricBalanceToMake(Mtrs)], [Fabric Completion Date],[FabricTargetDate], [LoomNo], [Loom_Location],[LoomType],[Seq], [ChangeOver_start_date],[ChangeOverType],[ChangeOver_Start_time/Shift],[Production capacity per day(Mtrs)],[AuditDate] FROM [Algo8].[dbo].[Weaving_Planning]"
            self.df_demand = pd.read_sql(query, self.engine)
            
            df_running = self.df_demand.loc[self.df_demand['Seq']=='Running']
            self.df_demand = self.df_demand.loc[~(self.df_demand['Seq']=='Running')]
            
            query = "SELECT FabricId, LoomNo,FabricTargetDate FROM [Algo8].[dbo].[ChangeOver_Freeze_Form2] WHERE Freeze = 'True'"
            freeze_fabric = pd.read_sql(query, self.engine)
            freeze = freeze_fabric.merge(self.df_demand,on=['FabricId','LoomNo','FabricTargetDate'],how='inner')
            
            self.df_demand = pd.concat([df_running,freeze],axis=0)
            
            self.df_demand['ChangeOver_start_date'].fillna(self.df_demand['AuditDate'], inplace=True)

            self.df_demand['ChangeOver_start_date'] = pd.to_datetime(self.df_demand['ChangeOver_start_date'])
            self.df_demand = self.df_demand[~(self.df_demand["AuditDate"]>= self.df_demand["Fabric Completion Date"])]  
            
            query="SELECT FabricId, LoomNo, PreLogic_FinalFactor FROM [Algo8].[dbo].[ExclusionCriteria_AllFabrics]"

            self.df_layers = pd.read_sql(query, self.engine)

            self.df_demand = pd.merge(self.df_demand, self.df_layers, on=['FabricId','LoomNo'],how='left')

            #adding loom location
            query = "SELECT * FROM [Algo8].[dbo].[LoomNo_and_Location]"
            self.loom_location = pd.read_sql(query, self.engine)


            self.df_demand.loc[self.df_demand['Seq'] == 'Running', 'Loom_Location'] = self.df_demand.loc[self.df_demand['Seq'] == 'Running', 'LoomNo'].map(self.loom_location.set_index('LoomNo')['Loom_Location'])
            self.Logs.Logging("File: Demand , Function: read_weaving_planning, Status : excecuted")
        except Exception as e :
            print(e)
            self.Logs.Logging(f"File: Demand, Function: read_weaving_planning, Status: not completed  Reason : {e}")
        
        return self.df_demand
    

    def calculate_no_of_looms(self, df_demand,df_ww):
        fabric_ids = list(df_demand['FabricId'])
        No_of_Looms = []
        for fabric_id in fabric_ids:
            filtered_df = df_demand.loc[(df_demand['FabricId'] == fabric_id)].copy()
            No_of_Looms.append(len(filtered_df['LoomNo'].unique()))
        df_demand['NoOfLooms'] = No_of_Looms
        df_demand=pd.merge(df_demand,df_ww,on=['FabricId'],how='left')
        return df_demand
    
    def read_master_fabric_data(self,df_demand):
        try :
            query = "SELECT FabricId, FabricWidth, FabricGSM, FabricWarpMesh, FabricWeftMesh,FabricTypeMultiplyingFactor FROM [Algo8].[dbo].[Master_Fabric]"
            self.df_1_Master = pd.read_sql(query, self.engine)
            df1 = pd.merge(df_demand,self.df_1_Master,on="FabricId",how='left')
            self.Logs.Logging("File: Demand , Function: read_master_fabric_data, Status : excecuted")
        except Exception as e :
            print(e)
            self.Logs.Logging(f"File: Demand, Function: read_master_fabric_data, Status: not excecuted  Reason : {e}")
        return df1

    #Read master tapes for properties of tape
    def read_master_tape_data(self,df):
        try :
            query = "SELECT [TapeId], [TapeDenier] FROM [Algo8].[dbo].[Master_Tape]"
            self.TapeDenier = pd.read_sql(query, self.engine)
            
            self.TapeDenier.rename(columns={'TapeId':'warp_id','TapeDenier':'Warp_Denier'},inplace=True)
            self.TapeDenier = self.TapeDenier.drop_duplicates()
            df = pd.merge(df, self.TapeDenier, on='warp_id', how='left')
            
            self.TapeDenier.rename(columns={'warp_id':'weft_id','Warp_Denier':'Weft_Denier'},inplace=True)
            self.TapeDenier = self.TapeDenier.drop_duplicates()
            df1 = pd.merge(df, self.TapeDenier, on='weft_id', how='left')
            
            self.TapeDenier.rename(columns={'weft_id':'warp_rf_id','Weft_Denier':'Warp_rf_Denier'},inplace=True)
            self.TapeDenier = self.TapeDenier.drop_duplicates()
            
            df2 = pd.merge(df1, self.TapeDenier, on='warp_rf_id', how='left')
            self.Logs.Logging("File: Demand, Function: read_master_tape_data, Status: excecuted")
        except Exception as e :
            print(e)
            self.Logs.Logging(f"File: Demand, Function: read_master_tape_data, Status: not excecuted  Reason : {e}")
        
        return df2
    
    def days_demand(self,df1):
        try :
            query = 'SELECT ConfiguredDays_Demand FROM [Algo8].[dbo].[Tapeline_ConfiguredDays_Demand_Schedule]'
            self.confg_days = pd.read_sql(query, self.engine)
            days = self.confg_days['ConfiguredDays_Demand'].values[0]
            
            running_fabrics = df1[df1['ChangeOver_start_date'].isnull()]
            running_fabrics['ULFabricBalanceToMake(Mtrs)'] = running_fabrics.apply(
                lambda x: min(x['ULFabricBalanceToMake(Mtrs)'], x['Production capacity per day(Mtrs)'] * days), axis=1)
            
            upcoming_changeovers = df1[(df1['ChangeOver_start_date'].notnull()) & (df1['ChangeOver_start_date'] < (pd.Timestamp.now() + pd.Timedelta(days=days)))]
            upcoming_changeovers['ULFabricBalanceToMake(Mtrs)'] = upcoming_changeovers.apply(
                lambda x: min(x['ULFabricBalanceToMake(Mtrs)'], x['Production capacity per day(Mtrs)'] * days), axis=1)
            
            df_new= pd.concat([running_fabrics,upcoming_changeovers])
            self.Logs.Logging("File: Demand, Function: days_demand, Status: excecuted")
        except Exception as e :
            print(e)
            self.Logs.Logging(f"File: Demand, Function: days_demand, Status: not excecuted  Reason : {e}")
        
        return df_new 
    
    def calculate_tape_demand(self, df):
        try : 
            df["FabricWidth"] = df["FabricWidth"] / 100
            df["FabricInKGs"] = np.round((df["FabricWidth"] * df["ULFabricBalanceToMake(Mtrs)"] * df["FabricGSM"]*df['FabricTypeMultiplyingFactor']) / 1000, 2)

            df['Warp_Tape'] = 0.0
            df['Warp_rf_Tape'] = 0.0
            df['Weft_Tape'] = 0.0

            # Iterate through the DataFrame and calculate 'Warp_Tape' iteratively
            for index, row in df.iterrows():
                df.at[index, 'Warp_Tape'] = round((row['FabricInKGs'] * row['Warp%']) / 100, 2)
                df.at[index, 'Warp_rf_Tape'] = round((row['FabricInKGs'] * row['Warp_rf%']) / 100, 2)
                df.at[index, 'Weft_Tape'] = round((row['FabricInKGs'] * row['Weft%']) / 100, 2)
            

            df["Tape_Target_Date"] = df['ChangeOver_start_date'] - pd.DateOffset(days=1) 
            df['Tape_Target_Date'] = pd.to_datetime(df['Tape_Target_Date'])
            df['Demand_Source'] = 'Weaving_Planning'
            
            dfk = df[['Previous_FabricId','Previous_FabricCode','FabricId', 'FabricCode', 'ChangeOver_start_date', 'Tape_Target_Date',
                   'ULFabricBalanceToMake(Mtrs)','ChangeOverType','Production capacity per day(Mtrs)', 'LoomNo','Loom_Location','LoomType','NoOfLooms', 
                   'Seq','warp_id', 'warp_name','warp_rf_id', 'warp_rf_name', 'weft_id', 'weft_name',
                    'Warp_Tape','Warp_rf_Tape', 'Weft_Tape',  
                   'FabricInKGs','FabricTypeMultiplyingFactor','Weft%','Warp%','Warp_rf%','PreLogic_FinalFactor',
                   'warp_no_of_tape_auto', 'warp_rf_no_of_tape_auto','Demand_Source']]
            
            #self.Logs.Logging("File: Demand, Function: calculate_tape_demand, Status: excecuted")
            
        except Exception as e :
            print(e)
            self.Logs.Logging(f"File: Demand, Function: calculate_tape_demand, Status: not excecuted  Reason : {e}")

        return dfk
    
    def add_prev_tape_data(self,dfk,df_ww):
        
        query="SELECT FabricId, LoomNo, PreLogic_FinalFactor FROM [Algo8].[dbo].[ExclusionCriteria_AllFabrics]"

        self.df_layers = pd.read_sql(query, self.engine)
        prev_layers = self.df_layers.copy()
        prev_layers.rename(columns = {'FabricId':'Previous_FabricId'},inplace=True)


        dfk1 = pd.merge(dfk,prev_layers,on=['Previous_FabricId','LoomNo'],how='left')

        dfk1.rename(columns={'PreLogic_FinalFactor_y':'prev_PreLogic_FinalFactor'},inplace=True)

        dfk1.rename(columns={'PreLogic_FinalFactor_x':'PreLogic_FinalFactor'},inplace=True)

        fab_recipe = df_ww.copy()
        fab_recipe.rename(columns={'FabricId':'Previous_FabricId'},inplace=True)

        dfk2 = pd.merge(dfk1,fab_recipe,on='Previous_FabricId',how ='left')


        dfk2.rename(columns={'warp_id_x':'warp_id','warp_name_x':'warp_name','warp_rf_id_x':'warp_rf_id',
                            'warp_rf_name_x':'warp_rf_name','weft_id_x':'weft_id','weft_name_x':'weft_name',
                            'Weft%_x':'Weft%','Warp%_x':'Warp%','Warp_rf%_x':'Warp_rf%',
                            'warp_no_of_tape_auto_x':'warp_no_of_tape_auto','warp_rf_no_of_tape_auto_x':'warp_rf_no_of_tape_auto'},inplace=True)

        dfk2.rename(columns={'warp_id_y':'prev_warp_id','warp_name_y':'prev_warp_name','warp_rf_id_y':'prev_warp_rf_id',
                            'warp_rf_name_y':'prev_warp_rf_name','weft_id_y':'prev_weft_id','weft_name_y':'prev_weft_name',
                            'Weft%_y':'prev_weft%','Warp%_y':'prev_warp%','Warp_rf%_y':'prev_warp_rf%',
                            'warp_no_of_tape_auto_y':'prev_warp_no_of_tape_auto','warp_rf_no_of_tape_auto_y':'prev_warp_rf_no_of_tape_auto'},inplace=True)
        return dfk2  
    
    def calculate_load_unload(self, dfk):
    
        dfk['prev_warp_rf_id'].fillna(0,inplace=True)
        dfk['warp_rf_id'].fillna(0,inplace=True)
        dfk['Prev_Warp_Unload'] = 0
        dfk['Prev_Warp_rf_Unload'] = 0
        dfk['Weft_Load'] = 0
        dfk['Warp_Load'] = 0
        dfk['Warp_rf_Load'] = 0
        dfk['Weft_Unload'] = 0
        dfk['Warp_Unload'] = 0
        dfk['Warp_rf_Unload'] = 0
        dfk['Warp_Change'] = ' '
        dfk['Warp_rf_Change'] = ' '
        dfk['Warp_Tape_Diff'] = 0
        dfk['Warp_rf_Tape_Diff'] = 0

        for index, row in dfk.iterrows():
            if row['warp_id'] != row['prev_warp_id']  :
                dfk.loc[index,'Warp_Change'] = 'Yes'
                dfk.loc[index, 'Warp_Load'] = row['warp_no_of_tape_auto'] * 1.5 * row['PreLogic_FinalFactor']
                dfk.loc[index, 'Warp_Unload'] = row['warp_no_of_tape_auto'] * 0.6 * row['PreLogic_FinalFactor']
                dfk.loc[index,'Prev_Warp_Unload'] =  row['prev_warp_no_of_tape_auto'] * 0.6 * row['prev_PreLogic_FinalFactor']
            else:
                dfk.loc[index,'Warp_Change'] = 'No'
                dfk.loc[index,'Warp_Tape_Diff']= row['warp_no_of_tape_auto'] - row['prev_warp_no_of_tape_auto']
                if row['warp_no_of_tape_auto'] > row['prev_warp_no_of_tape_auto']:  # increase in no of tapes
                    
                    dfk.loc[index, 'Warp_Load'] = (row['warp_no_of_tape_auto'] - row['prev_warp_no_of_tape_auto']) * 1.5 * row['PreLogic_FinalFactor']
                    dfk.loc[index,'Warp_Unload'] = row['warp_no_of_tape_auto'] * 0.6 * row['PreLogic_FinalFactor']
                    dfk.loc[index, 'Prev_Warp_Unload'] = 0
                elif row['warp_no_of_tape_auto'] < row['prev_warp_no_of_tape_auto']:  # decrease in no of tapes
                    
                    dfk.loc[index, 'Warp_Load'] = 0
                    dfk.loc[index, 'Warp_Unload'] = row['warp_no_of_tape_auto'] * 0.6 * row['PreLogic_FinalFactor']
                    dfk.loc[index, 'Prev_Warp_Unload'] = (row['prev_warp_no_of_tape_auto'] - row['warp_no_of_tape_auto']) * 0.6 * row['prev_PreLogic_FinalFactor']
                else :#no increase decrease
                    dfk.loc[index, 'Warp_Load'] = 0
                    dfk.loc[index, 'Warp_Unload'] =0
                    dfk.loc[index, 'Prev_Warp_Unload'] = row['prev_warp_no_of_tape_auto'] * 0.6 * row['prev_PreLogic_FinalFactor']
                
            if row['warp_rf_id'] != row['prev_warp_rf_id']:
                dfk.loc[index,'Warp_rf_Change'] = 'Yes'
                dfk.loc[index, 'Warp_rf_Load'] = row['warp_rf_no_of_tape_auto'] * 1.5 * row['PreLogic_FinalFactor']
                dfk.loc[index, 'Warp_rf_Unload'] = row['warp_rf_no_of_tape_auto'] * 0.6 * row['PreLogic_FinalFactor']
                dfk.loc[index,'Prev_Warp_rf_Unload'] =  row['prev_warp_rf_no_of_tape_auto'] * 0.6 * row['prev_PreLogic_FinalFactor']
            else:
                dfk.loc[index,'Warp_rf_Change'] = 'No'
                dfk.loc[index,'Warp_rf_Tape_Diff'] = row['warp_rf_no_of_tape_auto'] - row['prev_warp_rf_no_of_tape_auto']
                if row['warp_rf_no_of_tape_auto'] > row['prev_warp_rf_no_of_tape_auto']:  # increase in no of tapes
                    dfk.loc[index, 'Warp_rf_Load'] = (row['warp_rf_no_of_tape_auto'] - row['prev_warp_rf_no_of_tape_auto']) * 1.5 * row['PreLogic_FinalFactor']
                    dfk.loc[index,'Warp_rf_Unload'] = row['warp_rf_no_of_tape_auto'] * 0.6 * row['PreLogic_FinalFactor']
                    dfk.loc[index, 'Prev_Warp_rf_Unload'] = 0
                elif row['warp_rf_no_of_tape_auto'] < row['prev_warp_rf_no_of_tape_auto'] :  # decrease in no of tapes
                    dfk.loc[index, 'Warp_rf_Load'] = 0
                    dfk.loc[index, 'Warp_rf_Unload'] = row['warp_rf_no_of_tape_auto'] * 0.6 * row['PreLogic_FinalFactor']
                    dfk.loc[index, 'Prev_Warp_rf_Unload'] = (row['prev_warp_rf_no_of_tape_auto'] - row['warp_rf_no_of_tape_auto']) * 0.6 * row['prev_PreLogic_FinalFactor']
                else : #no increase decrease
                    dfk.loc[index, 'Warp_rf_Load'] = 0
                    dfk.loc[index, 'Warp_rf_Unload'] = 0
                    dfk.loc[index, 'Prev_Warp_rf_Unload'] = row['prev_warp_rf_no_of_tape_auto'] * 0.6 * row['prev_PreLogic_FinalFactor']
        dfk['Total_Warp_Demand'] = dfk['Warp_Tape'] + dfk['Warp_Load']
        dfk['Total_Weft_Demand'] = dfk['Weft_Tape']
        dfk['Total_Warp_rf_Demand'] = dfk['Warp_rf_Tape'] + dfk['Warp_rf_Load']
        return dfk
    
    def invent_df(self,demand_df):
        
        query = 'SELECT PlantProd_TapeId, Total FROM [Algo8].[dbo].[TapePlantStock_Gajner]'
        self.df_invent_GR = pd.read_sql(query, self.engine)
        self.df_invent_GR = self.df_invent_GR[self.df_invent_GR['Total'] > 0 ]



        query = 'SELECT PlantProd_TapeId, Total FROM [Algo8].[dbo].[TapePlantStock_D19]'
        self.df_invent_D19 = pd.read_sql(query, self.engine)
        self.df_invent_D19 = self.df_invent_D19[self.df_invent_D19['Total'] > 0 ]
        demand_df['Warp_Inventory_Utilised'] = 0
        demand_df['Weft_Inventory_Utilised'] = 0
        demand_df['Warp_rf_Inventory_Utilised'] = 0
        demand_df['Initial_Weft_Demand'] = demand_df['Total_Weft_Demand']
        demand_df['Initial_Warp_Demand'] = demand_df['Total_Warp_Demand']
        demand_df['Initial_Warp_rf_Demand'] = demand_df['Total_Warp_rf_Demand'] 
        for index, row in demand_df.iterrows():
            weft_id = row['weft_id']
            warp_id = row['warp_id']
            warp_rf_id = row['warp_rf_id']
            weft_demand = row['Total_Weft_Demand']
            warp_demand = row['Total_Warp_Demand']
            warp_rf_demand = row['Total_Warp_rf_Demand']
            
            if row['Loom_Location'] =='Gajner Road':
                
                if warp_id in self.df_invent_GR['PlantProd_TapeId'].values:
                    
                    
                    inventory = self.df_invent_GR.loc[self.df_invent_GR['PlantProd_TapeId'] == warp_id, 'Total'].values[0]
                    
                    i = self.df_invent_GR.loc[self.df_invent_GR['PlantProd_TapeId'] == warp_id].index[0]
                    
                    if warp_demand >= inventory :
                        demand_df.loc[index, 'Total_Warp_Demand'] = warp_demand - inventory
                        demand_df.loc[index,'Warp_Inventory_Utilised'] = inventory
                        self.df_invent_GR.loc[i, 'Total'] = 0
                    else :
                        demand_df.loc[index, 'Total_Warp_Demand'] = 0
                        demand_df.loc[index,'Warp_Inventory_Utilised'] = warp_demand
                        self.df_invent_GR.loc[i, 'Total'] = inventory- warp_demand
                    
                if weft_id in self.df_invent_GR['PlantProd_TapeId'].values:
                    inventory = self.df_invent_GR.loc[self.df_invent_GR['PlantProd_TapeId'] == weft_id, 'Total'].values[0]
                    i =self.df_invent_GR.loc[self.df_invent_GR['PlantProd_TapeId'] == weft_id].index[0]
                    if weft_demand >= inventory :
                        demand_df.loc[index, 'Total_Weft_Demand'] = weft_demand - inventory
                        demand_df.loc[index,'Weft_Inventory_Utilised'] = inventory
                        self.df_invent_GR.loc[i, 'Total'] = 0
                    else :
                        demand_df.loc[index, 'Total_Weft_Demand'] = 0 
                        demand_df.loc[index,'Weft_Inventory_Utilised'] = weft_demand
                        self.df_invent_GR.loc[i, 'Total'] = inventory - weft_demand

                if warp_rf_id in self.df_invent_GR['PlantProd_TapeId'].values:
                
                    inventory = self.df_invent_GR.loc[self.df_invent_GR['PlantProd_TapeId'] == warp_rf_id, 'Total'].values[0]
                    i = self.df_invent_GR.loc[self.df_invent_GR['PlantProd_TapeId'] == warp_rf_id].index[0]
                    
                    if warp_rf_demand >= inventory :
                        demand_df.loc[index, 'Total_Warp_rf_Demand'] = warp_rf_demand - inventory
                        demand_df.loc[index,'Warp_rf_Inventory_Utilised'] = inventory
                        self.df_invent_GR.loc[i, 'Total'] = 0
                    else :
                        
                        demand_df.loc[index, 'Total_Warp_rf_Demand'] = 0  
                        demand_df.loc[index,'Warp_rf_Inventory_Utilised'] = warp_rf_demand
                        self.df_invent_GR.loc[i, 'Total'] = inventory- warp_rf_demand
                    
                        
            elif row['Loom_Location'] =='D-19':
                
                if warp_id in self.df_invent_D19['PlantProd_TapeId'].values:
                
                    inventory = self.df_invent_D19.loc[self.df_invent_D19['PlantProd_TapeId'] == warp_id, 'Total'].values[0]
                    
                    i = self.df_invent_D19.loc[self.df_invent_D19['PlantProd_TapeId'] == warp_id].index[0]
                    
                    if warp_demand >= inventory :
                        demand_df.loc[index, 'Total_Warp_Demand'] = warp_demand - inventory
                        demand_df.loc[index,'Warp_Inventory_Utilised'] = inventory
                        self.df_invent_D19.loc[i, 'Total'] = 0
                    else :
                        demand_df.loc[index, 'Total_Warp_Demand'] = 0
                        demand_df.loc[index,'Warp_Inventory_Utilised'] = warp_demand
                        self.df_invent_D19.loc[i, 'Total'] = inventory- warp_demand
                    
                if weft_id in self.df_invent_D19['PlantProd_TapeId'].values:
                    inventory = self.df_invent_D19.loc[self.df_invent_D19['PlantProd_TapeId'] == weft_id, 'Total'].values[0]
                    i =self.df_invent_D19.loc[self.df_invent_D19['PlantProd_TapeId'] == weft_id].index[0]
                    if weft_demand >= inventory :
                        demand_df.loc[index, 'Total_Weft_Demand'] = weft_demand - inventory
                        demand_df.loc[index,'Weft_Inventory_Utilised'] = inventory
                        self.df_invent_D19.loc[i, 'Total'] = 0
                    else :
                        demand_df.loc[index, 'Total_Weft_Demand'] = 0
                        demand_df.loc[index,'Weft_Inventory_Utilised'] = weft_demand 
                        self.df_invent_D19.loc[i, 'Total'] = inventory - weft_demand

                if warp_rf_id in self.df_invent_D19['PlantProd_TapeId'].values:
                    
                    inventory = self.df_invent_D19.loc[self.df_invent_D19['PlantProd_TapeId'] == warp_rf_id, 'Total'].values[0]
                    i = self.df_invent_D19.loc[self.df_invent_D19['PlantProd_TapeId'] == warp_rf_id].index[0]
                    
                    if warp_rf_demand >= inventory :
                        demand_df.loc[index, 'Total_Warp_rf_Demand'] = warp_rf_demand - inventory
                        demand_df.loc[index,'Warp_rf_Inventory_Utilised'] = inventory
                        self.df_invent_D19.loc[i, 'Total'] = 0
                    else :
                        
                        demand_df.loc[index, 'Total_Warp_rf_Demand'] = 0 
                        demand_df.loc[index,'Warp_rf_Inventory_Utilised'] = warp_rf_demand 
                        self.df_invent_D19.loc[i, 'Total'] = inventory- warp_rf_demand

        return demand_df        
    
    def calculate_demand_df(self,demand_df):
        weft_demand_df = demand_df[['FabricId','weft_id','weft_name','Loom_Location','LoomNo',
                                    'ChangeOver_start_date','Tape_Target_Date','Total_Weft_Demand','Weft_Inventory_Utilised',
                                    'LoomType','Weft_Tape','Weft_Load','Weft_Unload','Seq',
                                    
                                    'ULFabricBalanceToMake(Mtrs)','Production capacity per day(Mtrs)','PreLogic_FinalFactor',
                                    'Initial_Weft_Demand','Demand_Source']]
        
        weft_demand_df['TapePropertyName'] = 'Weft'
        weft_demand_df['Weft_Demand']= weft_demand_df['Total_Weft_Demand']
        weft_demand_df['Warp_Demand']= 0
        weft_demand_df['Warp_rf_Demand']= 0
        weft_demand_df.rename(columns={'weft_id': 'TapeId', 'weft_name': 'Tape_name','Total_Weft_Demand':'Total_Demand',
                                    'Initial_Weft_Demand':'Initial_Demand','Weft_Tape':'Tape(Kg)','Weft_Load':'Tape_Load',
                                    'Weft_Unload':'Tape_Unload','Weft_Inventory_Utilised':'Inventory_Utilised'},inplace=True)
        
        
        warp_demand_df = demand_df[['FabricId','warp_id','warp_name','Loom_Location','LoomNo',
                                    'ChangeOver_start_date', 'Tape_Target_Date','Total_Warp_Demand','Warp_Inventory_Utilised',
                                    'LoomType','Warp_Tape','Warp_Load','Warp_Unload','Seq',
                                    
                                    'ULFabricBalanceToMake(Mtrs)','Production capacity per day(Mtrs)','PreLogic_FinalFactor',
                                    'Initial_Warp_Demand','Demand_Source']]
        
        warp_demand_df['TapePropertyName'] = 'Warp'
        warp_demand_df['Weft_Demand']= 0
        warp_demand_df['Warp_Demand']= warp_demand_df['Total_Warp_Demand']
        warp_demand_df['Warp_rf_Demand']= 0
        warp_demand_df.rename(columns={'warp_id': 'TapeId',  'warp_name': 'Tape_name','Total_Warp_Demand':'Total_Demand',
                                    'Initial_Warp_Demand':'Initial_Demand','Warp_Tape':'Tape(Kg)','Warp_Load':'Tape_Load',
                                    'Warp_Unload':'Tape_Unload','Warp_Inventory_Utilised':'Inventory_Utilised'}, inplace=True)
        
        warp_rf_demand_df = demand_df[['FabricId','warp_rf_id','warp_rf_name','Loom_Location','LoomNo',
                                    'ChangeOver_start_date', 'Tape_Target_Date','Total_Warp_rf_Demand','Warp_rf_Inventory_Utilised',
                                    'LoomType','Warp_rf_Tape','Warp_rf_Load','Warp_rf_Unload',
                                    
                                    'ULFabricBalanceToMake(Mtrs)','Production capacity per day(Mtrs)','PreLogic_FinalFactor',
                                    'Initial_Warp_rf_Demand','Demand_Source']]
        
        warp_rf_demand_df['TapePropertyName'] = 'Warp_rf'
        warp_rf_demand_df['Weft_Demand']= 0
        warp_rf_demand_df['Warp_Demand']= 0
        warp_rf_demand_df['Warp_rf_Demand']= warp_rf_demand_df['Total_Warp_rf_Demand']
        
        warp_rf_demand_df= warp_rf_demand_df[warp_rf_demand_df['warp_rf_id']!=0]
        
        warp_rf_demand_df.rename(columns={'warp_rf_id': 'TapeId',  'warp_rf_name': 'Tape_name','Total_Warp_rf_Demand':'Total_Demand',
                                        'Initial_Warp_rf_Demand':'Initial_Demand','Warp_rf_Tape':'Tape(Kg)','Warp_rf_Load':'Tape_Load',
                                    'Warp_rf_Unload':'Tape_Unload','Warp_rf_Inventory_Utilised':'Inventory_Utilised'}, inplace=True)

        tape_demand_df = pd.concat([weft_demand_df,warp_demand_df,warp_rf_demand_df],ignore_index=True)

        tape_demand = tape_demand_df.groupby(['TapeId','FabricId','Tape_Target_Date','Loom_Location','LoomNo','LoomType']).agg({
            'Tape_name':'first',
            'TapePropertyName':'unique',
            'Seq' : 'first',
            'Total_Demand':'sum',
            'Initial_Demand':'sum',
            'Inventory_Utilised':'sum',
            'Tape(Kg)':'sum',
            'Weft_Demand':'sum',
            'Warp_Demand':'sum',
            'Warp_rf_Demand': 'sum',
            'Tape_Load' : 'sum',
            'Tape_Unload' : 'sum',
            'Production capacity per day(Mtrs)':'first',
            'ULFabricBalanceToMake(Mtrs)':'first',
            'PreLogic_FinalFactor' :'first',
            'Demand_Source' :'first'
        }).reset_index()
        
        return tape_demand
   
    def add_buffer_plan(self,demand_df2):
        query = 'SELECT * FROM [Algo8].[dbo].[Tapeline_BufferPlan]'

        self.buffer_demand = pd.read_sql(query,self.engine)

        self.buffer_demand.rename(columns={'Qty(Kg)':'Total_Demand','TargetDate':'Tape_Target_Date'},inplace=True)
        self.buffer_demand['Demand_Source'] ='Buffer_demand'
        self.buffer_demand['Loom_Location'] = 'Gajner Road'
        
        query = 'SELECT TapeDescription, TapeId FROM [Algo8].[dbo].[Master_TapeRecipe]'
        tape_desc = pd.read_sql(query,self.engine)
        tape_desc['TapeId'] = pd.to_numeric(tape_desc['TapeId'], errors='coerce')
        self.buffer_demand = pd.merge(self.buffer_demand,tape_desc,on='TapeId',how='left')
        self.buffer_demand.drop_duplicates(inplace=True)
        self.buffer_demand.rename(columns={'TapeDescription':'Tape_name'},inplace=True)

        self.buffer_demand = self.buffer_demand[['TapeId','Tape_name','Tape_Target_Date','Total_Demand','Demand_Source','Loom_Location']]

    
        self.buffer_demand = self.buffer_demand[self.buffer_demand['Tape_Target_Date']<= (pd.Timestamp.now() + pd.Timedelta(days=self.confg_days['ConfiguredDays_Demand'].values[0]))]

        demand_df2= pd.concat([demand_df2,self.buffer_demand])
        
        return demand_df2
    
    def read_master_tape_data_extended(self):
        query = "SELECT TapeId, TapeWidth, TapeColour, TapeDenier, TapeMarking FROM [Algo8].[dbo].[Master_Tape]"
        self.dfkk = pd.read_sql(query, self.engine)
        return self.dfkk

    #Read Master Fabric
    def read_master_fabric_data_extended(self):
        query = "SELECT FabricId, FabricWarpMesh FROM [Algo8].[dbo].[Master_Fabric]"
        self.dfk = pd.read_sql(query, self.engine)
        return self.dfk

    #Read Master tape recipe
    def read_master_tape_recipe(self):
        #query = "SELECT Master_TapeRecipe_TapeId, Master_TapeRecipe_TapeMaterialId, Master_TapeRecipe_Percentage FROM [Algo8].[dbo].[Master_TapeRecipe]"
        query = "SELECT TapeId, MaterialId, Master_TapeRecipe_Percentage FROM [Algo8].[dbo].[Master_TapeRecipe]"
        self.dfkkk = pd.read_sql(query, self.engine)
        self.dfkkk = self.dfkkk.loc[self.dfkkk.groupby('TapeId')['Master_TapeRecipe_Percentage'].idxmax()]
        self.dfkkk = self.dfkkk.drop(['Master_TapeRecipe_Percentage'], axis=1)
        #dfkkk = dfkkk.rename(columns={'Master_TapeRecipe_TapeId': 'TapeId', 'Master_TapeRecipe_TapeMaterialId': 'MaterialId'})
        return self.dfkkk
    
    def process_demand_df2(self,demand_df3,dfkk,dfk):
        try:
            query ="SELECT TapeId , Master_TapeRecipe_Percentage, Master_TapeRecipe_ApplicableFrom, MaterialName, IsRP FROM [Algo8].[dbo].[Master_TapeRecipe1] where MaterialName like '%Filler%'"
            self.df_filler = pd.read_sql(query, self.engine)

            self.df_filler.dropna(inplace=True)
            self.df_filler['Master_TapeRecipe_ApplicableFrom'] = pd.to_datetime(self.df_filler['Master_TapeRecipe_ApplicableFrom'])

            # Group by 'TapeId' and find the row with the maximum date
            result = self.df_filler.sort_values('Master_TapeRecipe_ApplicableFrom', ascending=False).drop_duplicates('TapeId')[['TapeId', 'Master_TapeRecipe_Percentage']]
            result.rename(columns={'Master_TapeRecipe_Percentage':'TapeFiller'},inplace=True)
            demand_df3 = pd.merge(demand_df3,result,on='TapeId',how='left')

            query ="SELECT TapeId , Master_TapeRecipe_Percentage, Master_TapeRecipe_ApplicableFrom, MaterialName, IsRP FROM [Algo8].[dbo].[Master_TapeRecipe1] where MaterialName like '%REP%'"
            self.df_RP = pd.read_sql(query, self.engine)

            self.df_RP.dropna(inplace=True)
            self.df_RP['Master_TapeRecipe_ApplicableFrom'] = pd.to_datetime(self.df_RP['Master_TapeRecipe_ApplicableFrom'])

            # Group by 'TapeId' and find the row with the maximum date
            result = self.df_RP.sort_values('Master_TapeRecipe_ApplicableFrom', ascending=False).drop_duplicates('TapeId')[['TapeId', 'Master_TapeRecipe_Percentage']]

            result.rename(columns={'Master_TapeRecipe_Percentage':'TapeRP'},inplace=True)

            demand_df3 = pd.merge(demand_df3,result,on='TapeId',how='left')


            demand_df3.fillna(0,inplace=True)
            
            demand_df3 = pd.merge(demand_df3, dfkk, on='TapeId')
            demand_df3 = pd.merge(demand_df3, dfk, on='FabricId', how='left')

            #Extract the exact tape color name.  
            Tape_colour = demand_df3['TapeColour']
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

            demand_df3['TapeColour'] = Tape_colour_updated

            #Defining the Denier to be low if it is <1000 else high
            Tape_Denier = []
            TapeDenier = demand_df3['TapeDenier']
            for denier in TapeDenier:
                if denier <= 1000:
                    Tape_Denier.append('Low')
                else:
                    Tape_Denier.append('High')
            demand_df3['Tape_Denier'] = Tape_Denier

            #Defing the width to narrow, standard and wider
            Tape_width = []
            TapeWidth = demand_df3['TapeWidth']
            
            demand_df3['Tape_Width'] = demand_df3['TapeWidth'].round(2)
            for width in TapeWidth:
                if width < 2.5:
                    Tape_width.append('Narrow')
                elif 2.5 <= width <= 3.1:
                    Tape_width.append('Standard')
                else:
                    Tape_width.append('Wider')
            demand_df3['TapeWidth'] = Tape_width

            demand_df3 = demand_df3.sort_values(['Tape_Target_Date', 'TapeDenier'])

            #Extracting the value  Tape UV
            TapeUV = []
            for index, row in demand_df3.iterrows():
                tapename = row['Tape_name']
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
            demand_df3['TapeUv'] = TapeUV
            demand_df3['RPTAPE'] = np.where(demand_df3['TapeRP']==0, 0, 1)

            #adding UV values to buffer_plan tapes
            query ="SELECT TapeId , MaterialName, IsRP FROM [Algo8].[dbo].[Master_TapeRecipe1] where MaterialName like '%uv%'"
            self.UV_Tapes = pd.read_sql(query,self.engine)
            uv_tapes_set = set(self.UV_Tapes['TapeId'])
            
            demand_dfb =demand_df3[ demand_df3['Demand_Source']=='Buffer_demand']
            demand_dfb['TapeUv'] = demand_dfb.apply(lambda row: 1 if row['TapeId'] in uv_tapes_set else 0, axis=1)


            demand_df_final = pd.concat([demand_df3, demand_dfb],axis=0,ignore_index=True)
            
            demand_df_final['tapetype'] = demand_df_final['Tape_name'].apply(lambda x: 'Webbing' if 'Webbing' in x else ('Fabrilated' if 'Fabrilated' in x else 'Other'))
            demand_df_final.drop_duplicates(subset = ['TapeId','LoomNo','Tape_Target_Date'],inplace=True)
            
            self.Logs.Logging("File: Demand, Function: process_tape_demand, Status: excecuted")
            return demand_df_final
        except Exception as e :
            print(e)
            self.Logs.Logging(f"File: Demand, Function: process_tape_demand, Status: not excecuted  Reason : {e}")

    def upload_demand_to_DB(self,demand_final):
        try:
            #Initialized cursor to work with DB
            cursor = self.conx.cursor();
            #Truncating changeover table
            #cursor.execute("Truncate table Algo8_Tapeline_Demand")
            #Commited the task
            self.conx.commit()  
            #Closed Cursor
            cursor.close()
            print('Tables are Truncated!!')
            self.Logs.Logging("File: Demand, Function: upload_demand_to_DB, Status: Tables are successfullty Truncated")
        except Exception as e:
            self.Logs.Logging(f"File: Demand, Function: upload_demand_to_DB, Status: Tables are Not Truncated, Reason: {e}")

            raise "Not able to Truncate the tables"
        try: 
            demand_final["AuditDateTime"] = datetime.datetime.now()
            demand_final.to_sql(name = 'Algo8_Tapeline_Demand', con = self.engine, if_exists='append', index=False)
            print("Data  uploaded succesfully!")
            self.Logs.Logging("File: Demand, Function: upload_demand_to_DB, Status: Tables are added into DB")
        except Exception as e:
            print("Data not uploaded!")
            self.Logs.Logging(f"File: Demand, Function: upload_demand_to_DB, Status: Tables are Not added into DB, Reason: {e}")
            print(e)        
def calculate_tapes_demand():
    print("Calculating tape demand ")
    
    conn = Conn()
    conn.connect()
    engine = conn.get_engine()
    conx = conn.get_conx()
    
    calculator = TapeDemandCalculator(engine,conx)
    
    df_ww = calculator.read_fabric_recipe()
    
    df_demand = calculator.read_weaving_planning() 
    
    df_demand = calculator.calculate_no_of_looms(df_demand,df_ww)
    
    df1 = calculator.read_master_fabric_data(df_demand)

    df2 = calculator.read_master_tape_data(df1)

    df_new = calculator.days_demand(df2) 
    
    dfk = calculator.calculate_tape_demand(df_new)
    
    dfk2 = calculator.add_prev_tape_data(dfk,df_ww)
    
    demand_load_unload = calculator.calculate_load_unload(dfk2)
    
    demand_in = calculator.invent_df(demand_load_unload)
    
    demand_df2 = calculator.calculate_demand_df(demand_in)
    
    demand_df2 = calculator.add_buffer_plan(demand_df2)

    dfkk = calculator.read_master_tape_data_extended()
    dfk1 = calculator.read_master_fabric_data_extended()
    dfkkk = calculator.read_master_tape_recipe()
    
    
    demand_df3 = pd.merge(demand_df2, dfkkk, on='TapeId')
    
    demand_df_final = calculator.process_demand_df2(demand_df3, dfkk, dfk1)
    
    demand_df_final.to_excel("Demand_final_1.xlsx",index=False)
    
    demand = pd.read_excel("C:/Users/ATISHAY/Desktop/KPL_Tapeline/Demand_final_1.xlsx",index_col=False)
    
    calculator.upload_demand_to_DB(demand)
    
    return "Success"
  