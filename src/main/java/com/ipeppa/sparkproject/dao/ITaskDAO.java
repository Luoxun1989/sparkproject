package com.ipeppa.sparkproject.dao;

import com.ipeppa.sparkproject.domin.Task;

public interface ITaskDAO {
    Task findById(long taskid);
}
