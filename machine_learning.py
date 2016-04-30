import datetime
import scipy
import numpy
from sklearn import preprocessing
from sklearn import linear_model
from sklearn.cross_validation import StratifiedKFold
from sklearn.metrics import roc_auc_score
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier

def load_PDEs():
    #Input file was created using Pig to construct outcome from time window
    #by joining PDE's on Beneficiary death data
    print "reading file", datetime.datetime.now()
    f = open('inpatient_icd_6mo.txt', 'r')
    lines = f.readlines()
    f.close()
    return lines

def create_Input_Output(PDE_data):
    PDEs = []
    outcome = []
    print "starting file processing", datetime.datetime.now()
    for line in PDE_data:
        line = line.strip()
        line = line[1:-1]
        line = line.split(',')
        PDEs.append(line[2:5])
        outcome.append(line[-1])
    WIDTH = len(PDEs[0])
    print "starting X matrix", datetime.datetime.now()
    X = scipy.zeros((len(PDEs), WIDTH))
    for i in range(0, len(PDEs)):
        for j in range(0, WIDTH):
            X[i,j] = PDEs[i][j] if PDEs[i][j] != '' else 0
    print "median meds", numpy.median(X[:,0])
    print "median cost", numpy.median(X[:,1])
    print "median cost per time", numpy.median(X[:,2])
    
    Y = scipy.zeros(len(PDEs))
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
        X_train_scaled = preprocessing.scale(X[train])
        clf.fit(X_train_scaled, Y[train])
        X_test_scaled = preprocessing.scale(X[test])
        prediction_scaled = clf.predict_proba(X_test_scaled)
        aucs_baseline_scaled.append(roc_auc_score(Y[test], Y_baseline))
        aucs_scaled.append(roc_auc_score(Y[test], prediction_scaled[:,1]))
    print "baseline unscaled aucs", clf.__class__.__name__, aucs_baseline_unscaled, numpy.mean(aucs_baseline_unscaled)
    print "model unscaled aucs", clf.__class__.__name__, aucs_unscaled, numpy.mean(aucs_unscaled)
    print "diff unscaled aucs", clf.__class__.__name__, (numpy.mean(aucs_unscaled) - numpy.mean(aucs_baseline_unscaled))
    print "baseline scaled aucs", clf.__class__.__name__, aucs_baseline_scaled, numpy.mean(aucs_baseline_scaled)
    print "model scaled aucs", clf.__class__.__name__, aucs_scaled, numpy.mean(aucs_scaled)
    print "diff scaled aucs", clf.__class__.__name__, (numpy.mean(aucs_scaled) - numpy.mean(aucs_baseline_scaled))

def main():
    PDE_data = load_PDEs()
    X, Y = create_Input_Output(PDE_data)  

    print "starting Gaussian", datetime.datetime.now()
    clf = GaussianNB()
    test_prediction(clf, X, Y)

    print "starting SGD", datetime.datetime.now()
    clf = linear_model.SGDClassifier(loss='log')
    test_prediction(clf, X, Y)
    
    print "starting RF", datetime.datetime.now()
    clf = RandomForestClassifier(max_depth=15)
    test_prediction(clf, X, Y)

    print "finished", datetime.datetime.now()

if __name__ == '__main__':
    main()
