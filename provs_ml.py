import datetime
import scipy
import numpy
from sklearn import linear_model
from sklearn.cross_validation import StratifiedKFold
from sklearn.metrics import roc_auc_score
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier

def load_provs():
    f = open('prov_dates.txt', 'r')
    lines = f.readlines()
    f.close()
    return lines

def create_input_output(data):
    provs = []
    outcome = []
    print "begin processing...", datetime.datetime.now()
    for line in data:
        line = line.split(',')
        provs.append(line[2])
        outcome.append(line[3])
    WIDTH = len(provs[0])
    print "starting x matrix", datetime.datetime.now()
    X = scipy.zeros((len(provs), WIDTH))
    for i in range(0, len(provs)):
        for j in range(0, WIDTH):
            X[i,j] = provs[i][j] if provs[i][j] != '' else 0
    Y = scipy.zeros(len(provs))
    print "starting y matrix", datetime.datetime.now()
    for i in range(0, len(outcome)):
        Y[i] = outcome[i][0]
    return X, Y


def test_prediction(clf, X, Y):
    folds = StratifiedKFold(Y, 5)
    aucs = []
    for train, test in folds:
        clf.fit(X[train], Y[train])
        prediction = clf.predict_proba(X[test])
        aucs.append(roc_auc_score(Y[test], prediction[:,0]))
    print clf.__class__.__name__, aucs, numpy.mean(aucs)

            

def main():
    data = load_provs()
    X, Y = create_input_output(data)
    print "starting Gaussian", datetime.datetime.now()
    clf = GaussianNB()
    test_prediction(clf, X, Y)

    print "starting SGD", datetime.datetime.now()
    clf = linear_model.SGDClassifier(loss='log')
    test_prediction(clf, X, Y)

    print "starting RF", datetime.datetime.now()
    clf = RandomForestClassifier(max_depth = 15, class_weight = "balanced")
    test_prediction(clf, X, Y)

    print "finished", datetime.datetime.now()

if __name__ == '__main__':
    main()
