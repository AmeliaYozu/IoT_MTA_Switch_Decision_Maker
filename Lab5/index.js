// If this file contains double-curly-braces, that's because
// it is a template that has not been processed into JavaScript yet.
console.log('Loading event');
exports.handler = function(event, context) {
  var AWS = require('aws-sdk');
  var ml = new AWS.MachineLearning();
  var endpointUrl = 'https://realtime.machinelearning.us-east-1.amazonaws.com/';
  var mlModelId = 'ml-AHJA7TCEP2ZJ63RT';
  var numMessagesProcessed = 0;
  var numMessagesToBeProcessed = event.Records.length;

  console.log("numMessagesToBeProcessed:"+numMessagesToBeProcessed);

  var callPredict = function(mtaData){
    console.log('calling predict');
    console.log(mtaData);
    ml.predict(
      {
        Record : mtaData,
        PredictEndpoint : endpointUrl,
        MLModelId: mlModelId
      },
      function(err, data) {
        if (err) {
          console.log(err);
          context.done(null, 'Call to predict service failed.');
        }
        else {
          console.log('Predict call succeeded');
          console.log(data.Prediction.predictedLabel);
          context.done();
        }
      }
      );
  }

  var processRecords = function(){
    for(i = 0; i < numMessagesToBeProcessed; ++i) {
      encodedPayload = event.Records[i].kinesis.data;
      // Amazon Kinesis data is base64 encoded so decode here
      payload = new Buffer(encodedPayload, 'base64').toString('utf-8');
      try {
        parsedPayload = JSON.parse(payload);
        callPredict(parsedPayload);
      }
      catch (err) {
        console.log(err, err.stack);
        context.done(null, "failed payload"+payload);
      }
    }
  }

  var checkRealtimeEndpoint = function(err, data){
    if (err){
      console.log(err);
      context.done(null, 'Failed to fetch endpoint status and url.');
    }
    else {
      var endpointInfo = data.EndpointInfo;

      if (endpointInfo.EndpointStatus === 'READY') {
        endpointUrl = endpointInfo.EndpointUrl;
        console.log('Fetched endpoint url :'+endpointUrl);
        processRecords();
      } else {
        console.log('Endpoint status : ' + endpointInfo.EndpointStatus);
        context.done(null, 'End point is not Ready.');
      }
    }
  }

  ml.getMLModel({MLModelId:mlModelId}, checkRealtimeEndpoint);
};

