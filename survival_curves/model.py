import logging
import os
import sys
import pandas as pd
import numpy as np


def bp(billing_period, period, pf="", sf=""):
    return "%s%s%.2d%s" % (pf, billing_period, period, sf)


def avg_bp(billing_period, period):
    return bp(billing_period, period, sf="_mean")


def rate(billing_period, period):
    return bp(billing_period, period, "rate_", "_cum")

def rrate(billing_period, period):
    return bp(billing_period, period, "rrate_", "_cum")


def weight(period):
    return bp("weight", period)


def cohort_size_col(billing_period):
    return bp(billing_period, 1, sf="_count")


def cohort_safe_name(cohort_list):
    return "+".join(cohort_list)  # TODO: make it safer


def get_dtypes(input_file, periodicity):
    # improve pandas memory usage by specifying df data types on import
    periodCols=pd.read_csv(input_file, sep=";" , nrows=1) \
        .filter(regex='^'+periodicity).columns.to_list()
    input_types= {'tracking_id':'str',
                'joined_date':'str',
                'region':'str',
                'country':'str',
                'network_operator':'str',
                'pid' : 'int',
                'advertiser':'str',
                'advertiser_id':'int',
                'periodicity':'str',
                'price_point ':'float',
                'joined_week':'int'}
    input_types.update({x : 'float' for x in periodCols })
    return input_types

# TODO: Add configuration
class SurvivalCurveModel:
    periods = {'day': 365 + 1,
               'week': 52 + 1,
               'month': 12 + 1}

    log_config = {
        'format': '%(asctime)s %(levelname)s [%(filename)s:%(funcName)s:%(lineno)d] %(message)s',
        'datefmt': '%Y-%m-%dT%T',
        'level': 'INFO',
    }

    cohort_levels = [
        ["country"],
        ["country", "network_operator"],
        ["country", "network_operator", "advertiser"]
    ]
    @staticmethod
    def get_region_curves(periodicity):
        try:
            #return pd.read_pickle(os.path.join(os.path.abspath(os.path.dirname(__file__)), "data/REGION")) # TODO:must add option for monthly/weekly REGION
            if periodicity == "month":
                return pd.read_pickle(os.path.join(os.path.abspath(os.path.dirname(__file__)), "data/REGION_MONTH"))
            else:
                if periodicity == "week":
                    return pd.read_pickle(os.path.join(os.path.abspath(os.path.dirname(__file__)), "data/REGION_WEEK"))
                else:
                    print("periodicity must be month or week")
        except FileNotFoundError:
            return None

    @staticmethod
    def get_cohort_level_curves(raw_training, cohort, periodicity):
        timeserie = "joined_%s" % (periodicity)
        
        cohort_weight = 10 ** len(cohort)
        #cohort_weight = 1 * len(cohort)

        period_count = SurvivalCurveModel.periods[periodicity]

        # Group the data by the timeserie and cohort features
        grouping_aggregation = {bp(periodicity, 1): ["count", "mean"]}

        for period in range(2, period_count):
            grouping_aggregation[bp(periodicity, period)] = ["mean"]
        grouped = raw_training.groupby([timeserie] + cohort).agg(grouping_aggregation) ####
        grouped.columns = ['_'.join(tup).rstrip('_') for tup in grouped.columns.values]
        gr = grouped.reset_index()

        # IF max billing period not in the train set yet
        max_time = gr[timeserie].max()

        def get_max_bp(current_period, max_time, periodicity):
            max_year = int(max_time / 100)
            max_period = max_time - max_year * 100
            year = int(current_period / 100)
            period = current_period - year * 100
            if periodicity == "month":
                return (max_year - year) * 12 + (max_period - period)
            else:
                if periodicity == "week":
                    return (max_year - year) * 52 + (max_period - period)
                else:
                    #TODO: USE LOGGING instead of plain print
                    print("periodicity must be month or week")
        
        gr["max_bp"] = gr.apply(lambda x: get_max_bp(x[timeserie], max_time, periodicity), axis=1)
        g = gr.set_index([timeserie] + cohort)
        # Remove when max billing period is in the train set.
        g[rate(periodicity, 1)] = 1.0
        g[rrate(periodicity, 1)] = 1.0
        g.loc[g[avg_bp(periodicity, 1)] == 0, avg_bp(periodicity, 1)] = grouped[avg_bp(periodicity, 1)].mean()
        print
        for period in range(2, period_count):
            
            # applies retention rate 
            g[rrate(periodicity, period)] = g[avg_bp(periodicity, period)] \
                                            / g[avg_bp(periodicity, period-1)]

            g[rate(periodicity, period)] = g[rrate(periodicity, period)] \
                                            * g[rate(periodicity, period-1)]
        #applies attrition rate
        # for period in range(2, period_count):
        #     g[rate(periodicity, period)] = g[avg_bp(periodicity, period)] \
        #                                     / g[avg_bp(periodicity, 1)]
        # for period in range(2, period_count):
        #     g[rate(periodicity, period)] = g[rrate(periodicity, period)] \
        #                                     * g[rate(periodicity, period-1)]

        rate_list = [weight(period) for period in range(2, period_count)] + \
                    [rate(periodicity, period) for period in range(1, period_count)]


        averages = pd.DataFrame()
        weight_column = cohort_size_col(periodicity)
        gr = g.reset_index()

        for period in range(2, period_count):
            rate_col = rate(periodicity, period)

            #TODO: add a recent parameter for different business models (e.g. monthly)
            if period <= (period_count/2) :
                if periodicity != "month":
                    #use 4 more recent weeks to replicate finance model
                    recency_condition = (gr["max_bp"] >= period) & (gr["max_bp"] <= 4 + period)
                else:
                     recency_condition = (gr["max_bp"] >= period) & (gr["max_bp"] <= 1 + period)
            else: 
                recency_condition = (gr["max_bp"] >= period)

            valid_scope = gr[recency_condition]
            averages[rate_col] = valid_scope.groupby(cohort)[rate_col].mean()

            # ponderate curve weight by cohort volume
            # if cohort == ["country"]:
            #     global country_baseline
            #     country_baseline= valid_scope.groupby(cohort)[weight_column].sum()
            #     cohort_count=country_baseline
            # else:
            #     cohort_count= valid_scope.groupby(cohort)[weight_column].sum()
            #     print(country_baseline, cohort_count)
            #     if  country_baseline/cohort_count < 0.1:
                    # cohort_weight= cohort_weight/100
            
            averages[weight(period)] = valid_scope.groupby(cohort)[weight_column].sum() * cohort_weight

        averages[rate(periodicity, 1)] = 1.0

        return averages[rate_list].fillna(0.0)

    @staticmethod
    def shift_late_bill(df):
        #replace zero with nulls across DataFrame
        df.replace(0, np.nan, inplace=True)

        idx = np.isnan(df.values).argsort(axis=1)
        return pd.DataFrame(
                df.values[np.arange(df.shape[0])[:, None], idx],
                index=df.index,
                columns=df.columns,
            ).fillna(0)

    @staticmethod
    def train(input_file, output_file, periodicity):
        input_types=get_dtypes(input_file, periodicity)

        logging.info("training on [{}]".format(input_file))
        try:
            raw_training = pd.read_csv(input_file, sep=";" , index_col='tracking_id', dtype=input_types)
        except ValueError:
            print(pd.read_csv(input_file, sep=";").loc[[0]])
            sys.exit(1)

        for cohort in SurvivalCurveModel.cohort_levels:
            logging.info("get curves for cohort {} (periodicity level {})".format(cohort, periodicity))
            avg = SurvivalCurveModel.get_cohort_level_curves(raw_training, cohort, periodicity)

            logging.info("saving model to [{}]".format(output_file))
            os.makedirs(output_file, exist_ok=True)
            avg = avg.reset_index()
            avg.to_pickle(output_file + "/" + cohort_safe_name(cohort))

    @staticmethod
    def predict(input_file, model, output_file, periodicity):

        period_count = SurvivalCurveModel.periods[periodicity]

        def get_result_for_curves(df, curves, cohort, periodicity):
            logging.info("joining input data")

            #with pd.option_context('display.max_rows', 100):

            tdf = df.reset_index().merge(curves, on=cohort, how='left')
            #tdf.to_pickle("/tmp/"+cohort_safe_name(cohort))

            logging.info("Calculating subscriptions curves")
            for period in range(2, period_count):
                tdf[bp(periodicity, period)] = tdf[bp(periodicity, 1)] * \
                                               tdf[rate(periodicity, period)] * \
                                               tdf[weight(period)]
            return tdf.fillna(0.0)

        logging.info("predicting [{}] using [{}] model [{}]".format(input_file, periodicity, model))
        df = pd.read_csv(input_file, sep=";" , index_col='tracking_id')

        output_fields = ["tracking_id"] + \
                        [bp(periodicity, period) for period in range(1, period_count)]
        weights = [weight(period) for period in range(2, period_count)]

        results = []
        logging.info("loading region curves.")
        curves = SurvivalCurveModel.get_region_curves(periodicity)
        logging.info("getting results for curves")
        r = get_result_for_curves(df, curves, ["region"], periodicity)
        results.append(r[output_fields + weights])

        for cohort in SurvivalCurveModel.cohort_levels:
            logging.info("loading curves for cohort level {}.".format(cohort))
            curves = pd.read_pickle(model + "/" + cohort_safe_name(cohort))

            logging.info("getting results for curves")
            r = get_result_for_curves(df, curves, cohort, periodicity)

            logging.info("Appending results")
            results.append(r[output_fields + weights])


        logging.info("Concatenating results")
        r = pd.concat(results, sort=False)

        #r.to_pickle("/tmp/results_df.pickle")

        logging.info("Applying weighted averages")
        result_agg = {bp(periodicity, 1): 'mean'}
        for period in range(2, period_count):
            result_agg[bp(periodicity, period)] = 'sum'
            result_agg[weight(period)] = 'sum'
        results = r.groupby(["tracking_id"]).agg(result_agg)
        for period in range(2, period_count):
            results[bp(periodicity, period)] = results[bp(periodicity, period)] / results[weight(period)]

            # final weighted rate per transaction id  
            results[bp('rate', period, sf='final')]= results[bp(periodicity, period)]/results[bp(periodicity, 1)]

        # final rate columns
        rate_final= [bp('rate', period, sf='final') for period in range(2, period_count)]

        logging.info("saving results to [{}]".format(output_file))
        results.fillna(0.0).reset_index()[output_fields + rate_final].to_csv(output_file, sep=";", index=False)

    @staticmethod
    def consolidate(input_file, predictions_file, periodicity, model_version):

        # define output dimensions list and periods
        output_fields = ['tracking_id','joined_%s'%(periodicity),'region','country','network_operator','pid','advertiser','advertiser_id','periodicity','price_point']
        output_period = [bp(periodicity, period) for period in range(1, SurvivalCurveModel.periods[periodicity])]
        rate_final= [bp('rate', period, sf='final') for period in range(2, SurvivalCurveModel.periods[periodicity])]

        #loading prediction and input prediction file
        df1 = pd.read_csv(predictions_file, sep=';')
        df2 = pd.read_csv(input_file, sep=';')

        df1 = pd.merge(df1, df2[output_fields], on='tracking_id')
        output = pd.melt( df1, id_vars= output_fields, value_vars=output_period, var_name=periodicity, value_name='amount' )
        output['period']=pd.to_numeric(output[periodicity].str.strip(periodicity))
        output.drop(periodicity, inplace = True, axis = 1)
        output['model_version'] = model_version
        
        fields = output_fields[1:-1]
        final_curves = df1[fields + rate_final].drop_duplicates(subset=fields)
        final_curves['lifetime'] = final_curves[rate_final].sum(axis=1) + 1
        logging.info("saving predictions for curves to ['/data/final_curves_%s.csv']"%periodicity)
        final_curves.to_csv('/data/final_curves_%s.csv'%periodicity, sep=";", index=False)

        dimensions = ['joined_%s'%(periodicity),'region','country','network_operator','advertiser','periodicity', 'model_version']

        logging.info("saving predictions for backtest to [{}]".format(predictions_file))
        output.groupby(dimensions + ['period'])['amount'].sum().reset_index() \
              .rename(columns={'amount':'prediction'}).fillna(0.0) \
              .to_csv(predictions_file, index=False)


        # consolidation - setting data into customers PMAX CUBE format
        costs_table = '/data/pmax_costs_sample.csv' #find better location
        cost_group = ['network_operator','advertiser','country']
        df_costs=pd.read_csv(costs_table)

        # Group on week /maybe daily level and apply payout rate by Business Model
        consolidate = output.groupby(dimensions) \
                      .agg({'amount':'sum', 'tracking_id':lambda x: x.nunique()}) \
                      .reset_index() \
                      .rename(columns={'tracking_id':'volume'}) \

        # retrieve costs from the most granular group_cost level available up to country level
        for i in range (0 , len(cost_group)):
            consolidate = consolidate.merge(df_costs.groupby(cost_group[i:]) \
                        .agg({'Payout':'mean'}), on=cost_group[i:], how='left', suffixes=['','%s'%i])
            if i>0 and 'Payout%s' %i in consolidate.columns:
                consolidate['Payout'].fillna(consolidate['Payout%s' %i], inplace=True)
                consolidate.drop(['Payout%s' %i], axis=1, inplace=True)

        predictions_file_name, predictions_file_extension = os.path.splitext(predictions_file)
        consolidated_file = predictions_file_name + '_consolidated' + predictions_file_extension
        logging.info("saving consolidated results to [{}]".format(consolidated_file))
        clickhouse_order = ['joined_%s'%(periodicity), 'region', 'country', 'network_operator', 'advertiser', 'periodicity', 'amount','model_version', 'volume', 'Payout']
        convert_dict = {'joined_%s'%(periodicity) : str, 'region': str, 'country': str, 'network_operator': str,
                        'advertiser': str, 'periodicity': str, 'amount': float, 'model_version': str,
                        'volume': int, 'Payout':float
                        }
        consolidate = consolidate.astype(convert_dict)
        consolidate.fillna(0.0).reset_index()[clickhouse_order].to_csv(consolidated_file, sep=",", index=False)

    @staticmethod
    def main(arguments):
        #TODO: CHECK why is it needed
        # if arguments is None or len(arguments) < 4 or len(arguments) > 5:
        #     # TODO: usage (here)
        #     return 1

        logging.basicConfig(**SurvivalCurveModel.log_config)

        mode = arguments[1]
        input_file = arguments[2]
        output_file = arguments[3]
        periodicity = arguments[4]

        logging.info("mode is [{}]".format(mode))

        if mode == "train":
            SurvivalCurveModel.train(input_file, output_file, periodicity)
        elif mode == "predict":
            model = arguments[4]
            periodicity = arguments[5]
            model_version = arguments[6]
            SurvivalCurveModel.predict(input_file, model, output_file, periodicity)
            SurvivalCurveModel.consolidate(input_file, output_file, periodicity, model_version)
        else:
            # TODO: usage (here)
            return 1



