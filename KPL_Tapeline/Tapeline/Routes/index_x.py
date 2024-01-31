from flask import Blueprint
from ..Utilities.demand import calculate_tapes_demand
from ..Utilities.generate_schedule import get_schedule
#from ..Utilities.demand1 import Generate_Demand
##from ..Utilities.JCOPlanVsActual import Generate_PlanVsActual
#from ..Utilities.RMPlanVsActual import Generate_RMPlanVsActual

routes = Blueprint('routes' ,__name__)


routes.route('/kpl/tapeline/calculate_demand', methods = ['POST', 'GET'])(calculate_tapes_demand)
routes.route('/kpl/tapeline/generate_schedule', methods = ['POST', 'GET'])(get_schedule)
##routes.route('/al-garbia/schedule/refresh-schedule', methods = ['POST', 'GET'])(Generate_Demand)
#routes.route('/al-garbia/schedule/generate-schedule', methods = ['POST', 'GET'])(Generate_Schedule)
#routes.route('/al-garbia/schedule/jco-planvsactual', methods = ['POST', 'GET'])(Generate_PlanVsActual)
#routes.route('/al-garbia/schedule/rm-planvsactual', methods = ['POST', 'GET'])(Generate_RMPlanVsActual)
