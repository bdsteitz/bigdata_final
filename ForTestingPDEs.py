import datetime
import scipy
import numpy
from sklearn import preprocessing
from sklearn.metrics import confusion_matrix
from sklearn import linear_model
from sklearn.cross_validation import StratifiedKFold
from sklearn.cross_validation import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.metrics import precision_recall_curve
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier

def load_PDEs():
    #Input file was created using Pig to construct outcome from time window
    #by joining PDE's on Beneficiary death data
    print "reading file", datetime.datetime.now()
    f = open('ConvertedPatientFlags.out', 'r')
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
        PDEs.append(line[1:4])
        outcome.append(line[5])
    WIDTH = len(PDEs[0])
    print "starting X matrix", datetime.datetime.now()
    X = scipy.zeros((len(PDEs), WIDTH))
    for i in range(0, len(PDEs)):
        for j in range(0, WIDTH):
            X[i,j] = PDEs[i][j] if PDEs[i][j] != '' else 0
    Y = scipy.zeros(len(PDEs))
    print numpy.median(X[:,0])
    print numpy.median(X[:,1])
    print numpy.median(X[:,2])

    print "starting Y matrix", datetime.datetime.now()
    for i in range(0,len(outcome)):
        Y[i] = outcome[i][0]
    return X, Y

def test_prediction(clf, X, Y):
    folds = StratifiedKFold(Y, 5)
    aucs_baseline = []
    aucs = []
    prec_baseline = []
    prec = []
    rec_baseline = []
    rec = []
    for train, test in folds:
        Y_baseline = scipy.zeros(len(X[test]))
        clf.fit(X[train], Y[train])
        prediction = clf.predict_proba(X[test])
        aucs_baseline.append(roc_auc_score(Y[test], Y_baseline)) 
        aucs.append(roc_auc_score(Y[test], prediction[:,1]))
        precision_baseline, recall_baseline, threshold = precision_recall_curve(Y[test], Y_baseline)
        prec_baseline.append(precision_baseline)
        rec_baseline.append(recall_baseline)
        precision, recall, threshold = precision_recall_curve(Y[test], prediction[:,1])
        prec.append(precision)
        rec.append(recall)
    print "baseline aucs", clf.__class__.__name__, aucs_baseline, numpy.mean(aucs_baseline)
    print "calc aucs", clf.__class__.__name__, aucs, numpy.mean(aucs)
    print "diff aucs", clf.__class__.__name__, (numpy.mean(aucs) - numpy.mean(aucs_baseline))
    print "precision bl", clf.__class__.__name__, numpy.mean(prec_baseline)
    print "recall bl", clf.__class__.__name__, numpy.mean(rec_baseline)
    print "precision model", clf.__class__.__name__, numpy.mean(prec)
    print "recall model", clf.__class__.__name__, numpy.mean(rec)

def main():
    PDE_data = load_PDEs()
    X_unscaled, Y = create_Input_Output(PDE_data)
    
    #transform data
    print "scaling data", datetime.datetime.now()
    X = preprocessing.scale(X_unscaled)

    print "starting scaled Gaussian", datetime.datetime.now()
    clf = GaussianNB()
    test_prediction(clf, X, Y)

    print "starting non-scaled Gaussian", datetime.datetime.now()
    clf = GaussianNB()   
    test_prediction(clf, X_unscaled, Y)

    print "starting scaled SGD", datetime.datetime.now()
    clf = linear_model.SGDClassifier(loss='log')
    test_prediction(clf, X, Y)

    print "starting non-scaeld SGD", datetime.datetime.now()
    clf = linear_model.SGDClassifier(loss='log')
    test_prediction(clf, X_unscaled, Y)
    
    print "starting unscaled RF", datetime.datetime.now()
    clf = RandomForestClassifier(max_depth=25)
    test_prediction(clf, X_unscaled, Y)

    print "starting scaled RF", datetime.datetime.now()
    clf = RandomForestClassifier(max_depth=25)
    test_prediction(clf, X, Y)

    print "finished", datetime.datetime.now()

if __name__ == '__main__':
    main()
