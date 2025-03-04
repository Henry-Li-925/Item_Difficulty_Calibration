# coding: utf-8
# modified from https://github.com/bigdata-ustc/EduCDM/blob/main/EduCDM/IRT/GD/IRT.py
# 2025/03/01


import logging
import numpy as np
import torch
from torch import nn
import torch.nn.functional as F
from tqdm import tqdm
from sklearn.metrics import roc_auc_score, accuracy_score, f1_score

from ._irf import irf
from ._meta import Model

class IRTNet(nn.Module):
    def __init__(self, nuser, nitem, value_range, a_range, mode, irf_kwargs = None):
        super().__init__()
        self.nuser = nuser
        self.nitem = nitem
        self.mode = mode
        self.irf_kwargs = irf_kwargs if irf_kwargs is not None else {}
        self.a = nn.Embedding(self.nitem, 1)
        self.b = nn.Embedding(self.nitem, 1)
        self.c = nn.Embedding(self.nitem, 1)
        self.theta = nn.Embedding(self.nuser, 1)
        # Set the value range of theta/b: if 10, the value is transformed to the range between [-10,10]
        self.value_range = value_range 
        # Set the value range of a: if 10, the value is transformed to the range between [0,10]
        self.a_range = a_range


    def forward(self, user: torch.Tensor, item: torch.Tensor):
        theta = torch.squeeze(self.theta(user), -1)
        a = torch.squeeze(self.a(item), -1)
        b = torch.squeeze(self.b(item), -1)
        c = torch.squeeze(self.c(item), -1)
        c = torch.sigmoid(c) # constrain guessing parameter c to the range [0,1], keeping its monotonicity 
        if self.value_range:
            theta = self.value_range * (torch.sigmoid(theta) - 0.5)
            b = self.value_range * (torch.sigmoid(b) - 0.5)
        if self.a_range:
            a = self.a_range * torch.sigmoid(a)
        else:
            a = F.softplus(a) # Softplus transformation constrains the value to be always positive
        
        # Raise ValueError when the value range is so large that theta,a,b may contains nan!
        if torch.max(theta != theta) or torch.max(a != a) or torch.max(b != b):  
            raise ValueError('ValueError:theta,a,b may contains nan!  The value_range or a_range is too large.')
        
        return self.irt(theta, b, c, a, mode = self.mode, **self.irf_kwargs)

    
    # Transform the irf() function as the class method; the first argument should always be `cls`
    @classmethod
    def irt(cls, theta, b, c, a, mode, **kwargs):
        return irf(theta=theta, b=b, c=c, a=a, mode=mode, F=torch, **kwargs)


class IRT(Model):
    def __init__(self, nuser, nitem, value_range=None, a_range=None, mode = '3pl', irf_kwargs = None):
        super().__init__()
        self.irt_net = IRTNet(nuser, nitem, value_range, a_range, mode, irf_kwargs)

    def train(self, train_data, test_data = None, epoch = 2, device = 'cpu', lr = 0.001, wb_run = None):
        self.irt_net = self.irt_net.to(device)
        loss_fn = nn.BCELoss()
        self.loss_fn = loss_fn

        trainer = torch.optim.Adam(self.irt_net.parameters(), lr)
        self.trainer = trainer
        
        for e in range(1, epoch+1):
            losses = []
            for batch_data in tqdm(train_data, "Epoch %s" % e):
                user_id, item_id, response = batch_data
                user_id: torch.Tensor = user_id.to(device)
                item_id: torch.Tensor = item_id.to(device)
                response: torch.Tensor = response.to(device)
                predict_response = self.irt_net(user_id, item_id)
                loss = loss_fn(predict_response, response)
                

                # Backprop
                trainer.zero_grad()
                loss.backward()
                trainer.step()

                losses.append(loss.mean().item()) # append the mean loss of the batch; the value should be an original dtype value
                loss_value = float(np.mean(losses))
            print("Epoch %s:" % e)
            print("-" * 10 + "Training" + "-" * 10)
            print("Training loss: %.6f" % loss_value)

            # Logging if a W&B run is provided
            if wb_run:
                wb_run.log({
                    "loss": loss_value
                })

            if test_data:
                auc, acc, f1 = self.eval(test_data, device, wb_run)
                print("Epoch %s:" % e)
                print("-" * 10 + "Evaluating" + "-" * 10)
                print("AUC: %.6f" % auc)
                print("Acc: %.6f" % acc)
                print("F1: %.6f" % f1)


    def eval(self, test_data, device = 'cpu', wb_run = None):
        self.irt_net = self.irt_net.to(device)
        self.irt_net.eval()
        y_pred = []
        y_true = []

        for batch_data in tqdm(test_data, 'evaluating'):
            user_id, item_id, response = batch_data
            user_id: torch.Tensor = user_id.to(device)
            item_id: torch.Tensor = item_id.to(device)
            # Use extend method to iterate over values of iteratable, adding each element individually to the list. 
            y_true.extend(response.tolist())
            y_pred.extend(self.irt_net(user_id, item_id).tolist())
        
        self.irt_net.train()
        auc = roc_auc_score(y_true, y_pred)
        acc = accuracy_score(y_true, np.array([y > 0.5 for y in y_pred]))
        f1 = f1_score(y_true, np.array([y > 0.5 for y in y_pred]))

        # Logging if a W&B run is provided
        if wb_run:
            wb_run.log({
                "eval_auc": auc,
                "eval_acc": acc,
                "eval_f1": f1
            })
        return auc, acc, f1


    def save(self, filepath):
        torch.save(self.irt_net.state_dict(), filepath)
        logging.info("Save parameters to %s" % filepath)

    def load(self, filepath):
        self.irt_net.load_state_dict(torch.load(filepath))
        logging.info("Load parameters from %s" % filepath)

    def get_loss_fn(self):
        return self.loss_fn

    def get_trainer(self):
        return self.trainer
