package com.neandertal.cryptobot;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Iterator;
import java.util.Locale;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.neuroph.core.NeuralNetwork;
import org.neuroph.core.data.DataSet;
import org.neuroph.core.data.DataSetRow;
import org.neuroph.core.events.LearningEvent;
import org.neuroph.core.events.LearningEventListener;
import org.neuroph.core.events.NeuralNetworkEvent;
import org.neuroph.core.events.NeuralNetworkEventListener;
import org.neuroph.core.learning.LearningRule;
import org.neuroph.core.learning.SupervisedLearning;
import org.neuroph.nnet.MultiLayerPerceptron;
import org.neuroph.nnet.Perceptron;
import org.neuroph.nnet.learning.BackPropagation;
import org.neuroph.util.data.norm.Normalizer;

public class LearnStock
{
    private static final Logger logger = Logger.getLogger(LearnStock.class.getSimpleName());

    private static final String PATH_IN = "D:\\Workspace\\BTCbot\\in_data";
    private static final String PATH_OUT = "D:\\Workspace\\BTCbot\\out_data";
    private static final String PATH_NN = "D:\\Workspace\\BTCbot\\nn_data";

    private static class NNListener implements LearningEventListener
    {
        @Override
        public void handleLearningEvent(LearningEvent event)
        {
            BackPropagation bp = (BackPropagation) event.getSource();
            if (event.getEventType().equals(LearningEvent.Type.LEARNING_STOPPED))
            {
                System.out.println();
                System.out.println("Training completed in " + bp.getCurrentIteration() + " iterations");
                System.out.println("With total error " + bp.getTotalNetworkError() + '\n');
            }
            else
            {
                System.out.println("Iteration: " + bp.getCurrentIteration() + " | Network error: " + bp.getTotalNetworkError());
            }
        }
    }

    public static void main(String[] args)
    {
        int inputSize = 4;
        int outputSize = 1;
        int maxValue = 10000;
        try
        {
            DataSet trainSet = loadSet("btc_usd__coindesk.com.in", inputSize, outputSize);
            normalizeSet(trainSet, maxValue);
            NeuralNetwork nn = generateNN(inputSize, outputSize);
            learnNN(nn, trainSet);
            nn.save(PATH_NN + "\\test.nnet");
            
            DataSet testSet = loadSet("2017-08-30_2017-09-30_btc_usd__coindesk.in", inputSize, outputSize);
            normalizeSet(testSet, maxValue);
            
            DataSet resultSet = calculatePrediction(nn, testSet);
            printResultSet(testSet, resultSet, maxValue);
        }
        catch (Exception e)
        {
            logger.warning("Calculating failed!");
            e.printStackTrace();
        }
    }
    
    public static void printResultSet(DataSet testSet, DataSet resultSet, int maxValue)
    {
        for (DataSetRow row: resultSet.getRows())
        {
            double[] nInputs = row.getInput();
            for (int i = 3; i < nInputs.length; i++)
            {
                nInputs[i] = nInputs[i]*maxValue;
            }
            row.setInput(nInputs);
            
            double[] nOutputs = row.getDesiredOutput();
            for (int i = 0; i < nOutputs.length; i++)
            {
                nOutputs[i] = nOutputs[i]*maxValue;
            }
            row.setDesiredOutput(nOutputs);
        }
        
        System.out.println("\n>>> Results: ");
        System.out.println(resultSet.toString());
    }
    
    public static void normalizeSet(DataSet inputSet, int maxValue)
    {
        //Normalizing data set
        Normalizer normalizer = new Normalizer()
        {
            @Override
            public void normalize(DataSet set)
            {
                for (int i = inputSet.getRows().size() - 1; i >= 0; i--)
                {
                    DataSetRow row = inputSet.getRows().get(i);
                    double[] nInputs = row.getInput();
                    if (nInputs[0] < 1.0)
                    {
                        inputSet.remove(i);
                        continue;
                    }
                    
                    for (int j = 0; j < nInputs.length; j++)
                    {
                        nInputs[j] = nInputs[j]/maxValue;
                    }
                    row.setInput(nInputs);
                    
                    if (!set.isSupervised())
                    {
                        continue;
                    }
                    
                    double[] nOutputs = row.getDesiredOutput();
                    for (int j = 0; j < nOutputs.length; j++)
                    {
                        nOutputs[j] = nOutputs[j]/maxValue;
                    }
                    row.setDesiredOutput(nOutputs);
                }
                logger.info("Normalized set");
            }
        };
        normalizer.normalize(inputSet);
    }
    
    public static DataSet loadSet(String fileName, int inputSize, int outputSize) throws Exception
    {
        DataSet set = null;
        if (outputSize <= 0)
        {
            set = new DataSet(inputSize); 
        }
        else
        {
            set = new DataSet(inputSize, outputSize);
        }
        
        // read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(PATH_IN, fileName)))
        {
            int l = 0;
            Iterator<String> iter = stream.iterator();
            while (iter.hasNext())
            {
                String s = iter.next();
                
                s = s.trim();
                if (s.isEmpty()) break;
                
                String[] split = s.split(";");
                
                if (inputSize + outputSize != split.length)
                {
                    throw new Exception("Line: " + l + " in file: " + fileName + " does not have expected number of values");
                }
                
                l++;
                if (l == 1)//headers
                {
                    set.setColumnNames(split);
                    continue;
                }
                
                double[] inputValues = new double[inputSize];
                double[] outputValues = new double[outputSize];
                DataSetRow row = new DataSetRow();
                for (int i = 0; i < split.length; i++)
                {
                    String v = split[i];
                    double d;
                    try
                    {
                        NumberFormat format = NumberFormat.getInstance(Locale.ENGLISH);
                        Number number = format.parse(v);
                        d = number.doubleValue();
                    }
                    catch (ParseException e)
                    {
                        logger.warning("Failed to parse number: " + v + " in file: " + fileName);
                        break;
                    }
                    
                    if (i < inputSize)
                    {
                        inputValues[i] = d;
                    }
                    else if (outputSize > 0)
                    {
                        outputValues[i - inputSize] = d;
                    }
                }
                
                row.setInput(inputValues);
                row.setDesiredOutput(outputValues);
                set.add(row);
            }
            
            logger.info("Loaded " + fileName);
        }
        catch (Exception e)
        {
            logger.warning("Failed to read input from " + fileName);
            e.printStackTrace();
            return null;
        }
        return set;
    }

    public static NeuralNetwork generateNN(int inputSize, int outputSize)
    {
        // create new perceptron network
        MultiLayerPerceptron neuralNetwork = new MultiLayerPerceptron(inputSize, 2*inputSize + 1, outputSize);
        
        //get backpropagation learning rule from network
        BackPropagation learningRule = neuralNetwork.getLearningRule();
        learningRule.setLearningRate(0.4);
        learningRule.setMaxError(0.000001);
        learningRule.setMaxIterations(100000);
        
        learningRule.addListener(new NNListener());
        
        // learn the training set
        neuralNetwork.randomizeWeights();
        logger.info("Generated NN with in: " + inputSize + " and out: " + outputSize);
        return neuralNetwork;
    }
    
    public static NeuralNetwork learnNN(NeuralNetwork neuralNetwork, DataSet trainingSet)
    {
        neuralNetwork.learn(trainingSet);
        logger.info("Learn NN");
        return neuralNetwork;
    }

    public static DataSet calculatePrediction(NeuralNetwork neuralNetwork, DataSet inputSet) throws Exception
    {
        // load the saved network
        int input = neuralNetwork.getInputsCount();
        int output = neuralNetwork.getOutputsCount();

        if (inputSet.getInputSize() != input) 
        {
            throw new Exception("Input set has " + inputSet.getInputSize() + " columns, but NN expects " + input);
        }

        DataSet resultSet = new DataSet(input, output*2);
        resultSet.setColumnNames(inputSet.getColumnNames());
        for (DataSetRow row: inputSet.getRows())
        {
            neuralNetwork.setInput(row.getInput());
            neuralNetwork.calculate();
            double[] networkOutput = neuralNetwork.getOutput();
            
            double[] expectedAndActual = new double[output*2];
            System.arraycopy(row.getDesiredOutput(), 0, expectedAndActual, 0, row.getDesiredOutput().length);
            System.arraycopy(networkOutput, 0, expectedAndActual, row.getDesiredOutput().length, networkOutput.length);
            
            resultSet.add(new DataSetRow(row.getInput(), expectedAndActual));
        }
        
        logger.info("Calculated predictions.");
        return resultSet;
    }
}
