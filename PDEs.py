import datetime
import scipy
import numpy
from sklearn import linear_model
from sklearn.cross_validation import StratifiedKFold
from sklearn.metrics import roc_auc_score
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier
from sklearn import tree

def load_PDEs():
    #Input file was created using Pig to construct outcome from time window
    #by joining PDE's on Beneficiary death data
    f = open('Final_Processed_PDEs.out', 'r')
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
        outcome.append(line[5])
    WIDTH = len(PDEs[0])
    print "starting X matrix", datetime.datetime.now()
    X = scipy.zeros((len(PDEs), WIDTH))
    for i in range(0, len(PDEs)):
        for j in range(0, WIDTH):
            X[i,j] = PDEs[i][j] if PDEs[i][j] != '' else 0
    Y = scipy.zeros(len(PDEs))
    print "starting Y matrix", datetime.datetime.now()
    for i in range(0,len(outcome)):
        Y[i] = outcome[i][0]
    return X, Y

def test_prediction(clf, X, Y):
    folds = StratifiedKFold(Y, 5)
    aucs = []
    for train, test in folds:
        clf.fit(X[train],Y[train])
        prediction = clf.predict_proba(X[test])
        aucs.append(roc_auc_score(Y[test], prediction[:,1]))
    print clf.__class__.__name__, aucs, numpy.mean(aucs)

def main():
    PDE_data = load_PDEs()
    X, Y = create_Input_Output(PDE_data)
    
    print "starting Tree", datetime.datetime.now()
    clf = tree.DecisionTreeClassifier()
    test_prediction(clf, X, Y)

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
