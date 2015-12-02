Learning to Rank Solr
========

This is the main learning to rank integrated into solr repository.  [Read up on learning to rank](https://en.wikipedia.org/wiki/Learning_to_rank)



# Build the plugin
In the top level directory run
`mvn clean install`

# Install the plugin
In you solr installation, navigate to your collection's lib directory.  In the solr install example, it would be solr/collection1/lib.  If lib doesn't exist you will have to make it, then copy the plugin's jar there.

`cp target/ltr_solr-1.2-SNAPSHOT.jar mySolrInstallPath/solr/myCollection/lib`

Restart your collection using the admin page and you are good to go.  You can find more detailed instructions [here](https://wiki.apache.org/solr/SolrPlugins).

# Deploy Models and Features
To send features run 

`curl -XPUT 'http://localhost:8983/solr/collection1/config/fstore' --data-binary @/path/features.json -H 'Content-type:application/json'`

To send models run

`curl -XPUT 'http://localhost:8983/solr/collection1/config/mstore' --data-binary @/path/model.json -H 'Content-type:application/json'`

# Run a Rerank Query
Add to your original solr query 
`rq={!ltr model=myModelName reRankDocs=1000}`

The model name is the name of the model you sent to solr earlier.  The number of documents you want reranked, which can be larger than the number you display, is reRankDocs.
