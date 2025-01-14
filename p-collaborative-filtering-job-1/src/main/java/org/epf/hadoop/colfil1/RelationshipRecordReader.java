package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import java.io.IOException;

public class RelationshipRecordReader extends RecordReader<LongWritable, Relationship> {
    private LineRecordReader lineRecordReader;
    private LongWritable currentKey;
    private Relationship currentValue;

    public RelationshipRecordReader() {
        lineRecordReader = new LineRecordReader();
        currentKey = new LongWritable();
        currentValue = new Relationship();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) 
            throws IOException, InterruptedException {
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean hasNext = lineRecordReader.nextKeyValue();
        
        if (hasNext) {
            // Mettre à jour la clé
            currentKey.set(lineRecordReader.getCurrentKey().get());
            
            // Lire la ligne et la parser
            String line = lineRecordReader.getCurrentValue().toString();
            String relationship = line.split(",")[0];  // Utilise la virgule comme séparateur
            String[] users = relationship.split("<->");
            
            // Créer un nouvel objet Relationship
            currentValue = new Relationship(users[0], users[1]);
        }
        
        return hasNext;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Relationship getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}