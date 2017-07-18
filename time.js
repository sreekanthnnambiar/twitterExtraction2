var kafka = require('kafka-node')
var Producer = kafka.Producer
var client = new kafka.Client("localhost:2181/")

var twitterTopic = "twitter";
    KeyedMessage = kafka.KeyedMessage,
    producer = new Producer(client),
    km = new KeyedMessage('key', 'message'),
    countryProducerReady = false;




var Twit=require('twit');//we can get this package details from www.npmjs.com
var T = new Twit({
  consumer_key:         'q7jSHVoUPF655tBLSF4MMKKn6',
  consumer_secret:      'S6pxJm4EFXYSbTb6ctpyrecOvgDnE5VGa7sDrdtFuCVboXEw0E',
  access_token:         '877811802767216640-VYY3roSg2Ld5SCugt2GNiKnunUZ0Nbh',
  access_token_secret:  '6NZXPADZbQPsgYmJ7ufG6CkbaKh1SsiVRaTkYFiOyPoXp',
  timeout_ms:           60*1000,  // optional HTTP request timeout to apply to all requests.
})



var screenName=["HARDWELL","twitApp123","virendersehwag","narendramodi"];
var statusIDStore=[];
var lastAddedTime=0;
function getTweetsByScreenName(){
    getNextScreenName(screenName, 0,function(){
    console.log("end of statuses"+statusIDStore)
});

}

setInterval(getTweetsByScreenName,10000)
//getTweetsByScreenName();


function getNextScreenName(nameList,keyIndex,success){

    if(nameList.length==keyIndex)
    {
        lastAddedTime=Math.round(new Date().getTime());
        success();
    }
    else{
         sn=nameList[keyIndex];
    
    getNextStatus(sn,0,function(){
            keyIndex++;
        getNextScreenName(nameList,keyIndex,success)
        })

    }
   
    
}

function getNextStatus(key,index,done){
    var params={
    screen_name:key,
    count:2
    }

    T.get("statuses/user_timeline",params,function(err,data,response){

    
    if(!err){

            for(var i=0;i<data.length;i++){

                var createdTime=data[i].created_at;
                 var d = new Date(createdTime);
                 var timeStampValue=d.getTime();
                 var userIdTweets=data[i].user.id_str
                 var sId=data[i].id_str;
                 var status=getStatus(sId);

                 if(lastAddedTime==0){
                     status.id=sId;
                    status.name=data[i].text
                    status.timestamp=timeStampValue;
                    status.userIdTweeter=userIdTweets
                    statusIDStore.push(status);
                    console.log(data[i].user.name+"'s data added"+sId);
                 }
                 if(lastAddedTime>0){
                     
                        if(lastAddedTime<timeStampValue){
                            status.id=sId;
                            status.name=data[i].text
                            status.timestamp=timeStampValue;
                            status.userIdTweeter=userIdTweets
                            statusIDStore.push(status);
                            console.log(data[i].user.name+"'s data added "+sId);
                            
                        }
                        else{
                            console.log("already exist")
                        }

                 }
                 
                 
            }
            done();
            
    }
    else{
        console.log(err)
        done();
        
    }
      

});

}

function getStatus(id){
    if(statusIDStore==null||statusIDStore.length==0){
        var status1={};
        status1.id='';
        status1.name="";
        status1.timestamp='';
        status1.userIdTweeter='';
        return status1;
    }
    for(var i=0;i<statusIDStore.length;i++){
       if(statusIDStore[i].id==id){
           
           return statusIDStore[i];
       }
   }
   for(var i=0;i<statusIDStore.length;i++){
       if(statusIDStore[i].id!=id){
           var status1={};
           status1.id='';
           status1.name="";
           status1.timestamp='';
           status1.userIdTweeter='';
           return status1;
       }
   }

}

//------------------------------end of getting statusses--------------------------------
var userIdStoreJson=[];
function getRetweetersbyStatusID(){
    getNextStatusID(statusIDStore,0,function(){
        console.log("finished finding retweeters"+userIdStoreJson);
        console.log("");
    })
}
setInterval(getRetweetersbyStatusID,4000)
//getRetweetersbyStatusID();
function getNextStatusID(stId,stindex,done){
    if(stId.length==stindex){
        lastRetweeterUpdatedTime=Math.round(new Date().getTime());
        done();
    }
    else{
        statusID=stId[stindex]
        console.log(statusID);
        getNextRetweeter(statusID,0,function(){
            stindex++;
            getNextStatusID(stId,stindex,done)
        })
    }

}

function getNextRetweeter(status2,index,success){
    var statusId=status2.id;
    var statusName=status2.name;
    T.get('statuses/retweets/:id', {id:statusId,count:5 },function(error, data, response){

     if(!error){
    
        var status=getStatus2(status2);
        for(var i=0;i<data.length;i++){

            var createdTime=data[i].created_at;
            var d = new Date(createdTime);
            var timeStampValue=d.getTime();
            if(lastRetweeterUpdatedTime==0){
                var temp={};
                temp.userId=data[i].user.id_str;
                temp.userName=data[i].user.name;
                status.userIds.push(temp);
                var dataToKafka={};
                    dataToKafka.status=statusName;
                    dataToKafka.stusId=statusId;
                    dataToKafka.userName=data[i].user.name;

                    doKafka(dataToKafka,function(payloads){
                    producer.send(payloads, function (err, loadeddata) {
                    console.log(loadeddata);
                    console.log(payloads);

                    console.log("");
            });

        });

            }
            if(lastRetweeterUpdatedTime>0){
                if(lastRetweeterUpdatedTime<timeStampValue){

                    var temp={};
                    temp.userId=data[i].user.id_str;
                    temp.userName=data[i].user.name;
                    status.userIds.push(temp);
                    console.log(data[i].user.id_str+" new user added")

                    var dataToKafka={};
                    dataToKafka.status=statusName;
                    dataToKafka.stusId=statusId;
                    dataToKafka.userName=data[i].user.name;

                    doKafka(dataToKafka,function(payloads){
                    producer.send(payloads, function (err, loadeddata) {
                    console.log(loadeddata);
                    console.log(payloads);

                    console.log("");
            });

        });

        }
        else{
            console.log("Retweeter already exist")
            }
        }
                //-------------------------------------------------------
        function doKafka(dataToKafka,addData)
          {
             
             KeyedMessage = kafka.KeyedMessage,
             twitterKM = new KeyedMessage(dataToKafka.statussId, JSON.stringify(dataToKafka)),
             payloads = [
                      { topic: twitterTopic, messages: twitterKM, partition: 0 },
                     ];
          
             addData(payloads);
          }
            
        }
            success();
        }
        else{
            console.log("error in getting retweeters"+error);
            success();
        }

      })
      
}
function getStatus2(statusDetails)
{
   if(userIdStoreJson==null || userIdStoreJson.length==0)
   {
       var status={};
       status.Id=statusDetails.id;
       status.name=statusDetails.name;
       status.userIds=[];
       userIdStoreJson.push(status);
       return status;
   }
   for(var i=0;i<userIdStoreJson.length;i++){
       if(userIdStoreJson[i].Id==statusDetails.id){
           
           return userIdStoreJson[i];
       }
   }
   for(var i=0;i<userIdStoreJson.length;i++){

       if(userIdStoreJson[i].Id!=statusDetails.id){
           var status={};
           status.Id=statusDetails.id;
           status.name=statusDetails.name;
           status.userIds=[];
           userIdStoreJson.push(status);
           console.log("status id "+status.Id)
           return status;
       }

   }
}