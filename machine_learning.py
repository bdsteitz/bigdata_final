import datetime
import scipy
import numpy
from sklearn import preprocessing
from sklearn import linear_model
from sklearn.cross_validation import StratifiedKFold
from sklearn.metrics import roc_auc_score
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.feature_selection import RFE

#infile can be changed to process each data source independently
infile = 'Final_Model.out'

#NOTE: Feature selection with RFE and assessment of feature importance with RF 
#only done if eval_features is set to TRUE
eval_features = False

def load_infile():
    print "reading file", datetime.datetime.now()
    f = open(infile, 'r')
    lines = f.readlines()
    f.close()
    return lines

def create_Input_Output(feature_data):
    features = []
    outcome = []
    print "starting file processing", datetime.datetime.now()
    for line in feature_data:
        line = line.strip()
        if line[2] == '(':
            line = line[2:-2]
        elif line[1] == '(':
            line = line[1:-1]
        line = line.split(',')
        features.append(line[2:-1])
        outcome.append(line[-1])
    WIDTH = len(features[0])
    print "starting X matrix", datetime.datetime.now()
    X = scipy.zeros((len(features), WIDTH))
    for i in range(0, len(features)):
        for j in range(0, WIDTH):
            X[i,j] = features[i][j] if features[i][j] != '' else 0
    
    Y = scipy.zeros(len(features))
    print "starting Y matrix", datetime.datetime.now()
    for i in range(0,len(outcome)):
        Y[i] = outcome[i][0]
    return X, Y

def test_prediction(clf, X, Y):
    folds = StratifiedKFold(Y, 5)
    aucs_baseline_unscaled = []
    aucs_unscaled = []
    aucs_baseline_scaled = []
    aucs_scaled = []
    for train, test in folds:
        Y_baseline = scipy.zeros(len(X[test]))
        clf.fit(X[train], Y[train])
        prediction_unscaled = clf.predict_proba(X[test])
        aucs_baseline_unscaled.append(roc_auc_score(Y[test], Y_baseline))
        aucs_unscaled.append(roc_auc_score(Y[test], prediction_unscaled[:,1]))
        #scales data for models except in RF
        if clf.__class__.__name__ != "RandomForestClassifier":
            X_train_scaled = preprocessing.scale(X[train])
            clf.fit(X_train_scaled, Y[train])
            X_test_scaled = preprocessing.scale(X[test])
            prediction_scaled = clf.predict_proba(X_test_scaled)
            aucs_baseline_scaled.append(roc_auc_score(Y[test], Y_baseline))
            aucs_scaled.append(roc_auc_score(Y[test], prediction_scaled[:,1]))
    print "baseline unscaled aucs", clf.__class__.__name__, aucs_baseline_unscaled, numpy.mean(aucs_baseline_unscaled)
    print "model unscaled aucs", clf.__class__.__name__, aucs_unscaled, numpy.mean(aucs_unscaled)
    print "diff unscaled aucs", clf.__class__.__name__, (numpy.mean(aucs_unscaled) - numpy.mean(aucs_baseline_unscaled))
    if clf.__class__.__name__ != "RandomForestClassifier":
        print "baseline scaled aucs", clf.__class__.__name__, aucs_baseline_scaled, numpy.mean(aucs_baseline_scaled)
        print "model scaled aucs", clf.__class__.__name__, aucs_scaled, numpy.mean(aucs_scaled)
        print "diff scaled aucs", clf.__class__.__name__, (numpy.mean(aucs_scaled) - numpy.mean(aucs_baseline_scaled))

def main():
    start_time = datetime.datetime.now()
    feature_data = load_infile()
    X, Y = create_Input_Output(feature_data)  

    print "starting Gaussian", datetime.datetime.now()
    clf = GaussianNB()
    test_prediction(clf, X, Y)

    print "starting SGD", datetime.datetime.now()
    clf = linear_model.SGDClassifier(loss='log')
    test_prediction(clf, X, Y)
    
    print "starting RF", datetime.datetime.now()
    clf = RandomForestClassifier(max_depth=15)
    test_prediction(clf, X, Y)
    if eval_features:
        print "RF Features", clf.n_features_, clf.feature_importances_

    if eval_features:
        print "starting LR", datetime.datetime.now()
        clf = LogisticRegression()
        rfe = RFE(clf, 10)
        test_prediction(rfe, X, Y)
        print "RFE support and ranking", rfe.support_, rfe.ranking_

    print "finished", datetime.datetime.now()
    print "total runtime: ", datetime.datetime.now() - start_time
if __name__ == '__main__':
    main()
