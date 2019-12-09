#connection
require('RPostgreSQL')
drv<-dbDriver('PostgreSQL')
con<-dbConnect(drv,dbname='shopify_appstore',host='f19server.apan5310.com',port=50205,user='postgres',password='3utmhnzw')

#table1:apps
apps_data<-read.csv('C:/Users/luoyu/Desktop/SQL/project/shopify data/apps.csv',stringsAsFactors=F)
##apps_dataset
apps<-apps_data[,c('id','title','url','tagline','icon','pricing_hint')]
names(apps)<-c('app_id','app_title','url','tagline','icon','pricing_hint')
##check duplication:no duplication
apps[duplicated(apps[c('url')]),]
##write table
dbWriteTable(con,name='apps',value=apps,row.names=FALSE,append=TRUE)

#table2:benefits
benefits_data<-read.csv('C:/Users/luoyu/Desktop/SQL/project/shopify data/key_benefits.csv',stringsAsFactors=F)
names(benefits_data)<-c('app_id','benefit_title','benefit_description')
##check duplication:each app may have several benefits
benefits_data[duplicated(benefits_data['app_id']),]
##check duplication:different apps may have same benefits
benefits_data[duplicated(benefits_data[c('benefit_title','benefit_description')]),]
##unique
benefits_unique<-benefits_data[,c('benefit_title','benefit_description')]
benefits_unique<-unique(benefits_unique)
##double check duplication:no duplication
benefits_unique[duplicated(benefits_unique[c('benefit_title','benefit_description')]),]
##add benefit_id
benefits_unique$benefit_id<-1:nrow(benefits_unique)
##benefits_dataset
benefits<-benefits_unique[,c('benefit_id','benefit_title','benefit_description')]
##write table
dbWriteTable(con,name='benefits',value=benefits,row.names=FALSE,append=TRUE)

#table3:apps_and_benefits
##mapping benefit_id and app_id
benefits_data<-merge(benefits_data,benefits_unique[,c('benefit_id','benefit_title','benefit_description')],by=c('benefit_title','benefit_description'))
##apps_and_benefits dataset
apps_and_benefits<-benefits_data[,c('app_id','benefit_id')]
##write table
dbWriteTable(con,name='apps_and_benefits',value=apps_and_benefits,row.names=FALSE,append=TRUE)

#table4:categories
categories_data<-read.csv('C:/Users/luoyu/Desktop/SQL/project/shopify data/categories.csv',stringsAsFactors=F)
names(categories_data)<-c('category_id','category_title')
##categories dataset
categories<-categories_data
##write table
dbWriteTable(con,name='categories',value=categories,row.names=FALSE,append=TRUE)

#table5:apps_and_categories
apps_and_categories_data<-read.csv('C:/Users/luoyu/Desktop/SQL/project/shopify data/apps_categories.csv',stringsAsFactors=F)
##apps_and_categories dataset
apps_and_categories<-apps_and_categories_data
##write table
dbWriteTable(con,name='apps_and_categories',value=apps_and_categories,row.names=FALSE,append=TRUE)

#table6:descriptions
descriptions_data<-apps_data[,c('id','description','description_raw')]
names(descriptions_data)<-c('app_id','description','description_raw') 
##check duplication:no duplication
descriptions_data[duplicated(descriptions_data['description']),]
##add description_id
descriptions_data$description_id<-1:nrow(descriptions_data)
##descriptions dataset
descriptions<-descriptions_data[,c('description_id','description','description_raw')]
##write table
dbWriteTable(con,name='descriptions',value=descriptions,row.names=FALSE,append=TRUE)

#table7:apps_and_descriptions
##apps_and_descriptions dataset
apps_and_descriptions<-descriptions_data[,c('app_id','description_id')]
##write table
dbWriteTable(con,name='apps_and_descriptions',value=apps_and_descriptions,row.names=FALSE,append=TRUE)

#table8:developers
developers_data<-apps_data[,c('developer','developer_link')]
##check duplication:some apps may have the same developers
developers_data[duplicated(developers_data[c('developer','developer_link')]),]
developers_data<-unique(developers_data)
##add developer_id
developers_data$developer_id<-1:nrow(developers_data)
##write table
dbWriteTable(con,name='developers',value=developers_data,row.names=FALSE,append=TRUE)

#table9:apps_and_devlopers
apps_and_developers<-apps_data[,c('id','developer','developer_link')]
apps_and_developers<-merge(apps_and_developers,developers_data[,c('developer_id','developer','developer_link')],by=c('developer','developer_link'))
names(apps_and_developers)<-c('developer','developer_link','app_id','developer_id')
##write table
dbWriteTable(con,name='apps_and_developers',value=apps_and_developers[,c('app_id','developer_id')],row.names=FALSE,append=TRUE)

#table10:pricing_features
Sys.setlocale("LC_ALL", "English")
pricing_plan_feature_data = read.csv('C:/Users/luoyu/Desktop/SQL/project/shopify data/pricing_plan_features.csv',stringsAsFactors=F)
features_data = data.frame('feature' = unique(pricing_plan_feature_data$feature))
features_data$pricing_feature_id = 1:nrow(features_data)
##write table
dbWriteTable(con,name='pricing_features',value=features_data,row.names=FALSE,append=TRUE)

#table11:apps_and_pricing_features
pricing_feature_id_list = sapply(pricing_plan_feature_data$feature, function(x) features_data$pricing_feature_id[features_data$feature == x])
pricing_plan_feature_data$pricing_feature_id = pricing_feature_id_list
apps_and_pricing_features_data = pricing_plan_feature_data[c('app_id','pricing_feature_id')]
apps_and_pricing_features_data = unique(apps_and_pricing_features_data)
##write table
dbWriteTable(con,name='apps_and_pricing_features',value=apps_and_pricing_features_data,row.names=FALSE,append=TRUE)

#table12:pricing_plans
pricing_plans_data = read.csv('C:/Users/luoyu/Desktop/SQL/project/shopify data/pricing_plans.csv',stringsAsFactors=F)
names(pricing_plans_data)<-c('pricing_plan_id_original','app_id','title','price_plan')
pricing_plans_unique = unique(pricing_plans_data[c('price_plan', 'title')])
pricing_plans_unique$pricing_plan_id<-1:nrow(pricing_plans_unique)
##write table
dbWriteTable(con,name='pricing_plans',value=pricing_plans_unique,row.names=FALSE,append=TRUE)

#table13:apps_and_pricing_plans
apps_and_pricing_features_data = merge(pricing_plans_data,pricing_plans_unique,by=c('price_plan','title'))
apps_and_pricing_plans_data = apps_and_pricing_features_data[c('app_id', 'pricing_plan_id')]
##write table
dbWriteTable(con,name='apps_and_pricing_plans',value=apps_and_pricing_plans_data,row.names=FALSE,append=TRUE)

#table14:pricing_plans_and_pricing_features
##mapping feature_id
plans_and_features<-merge(pricing_plan_feature_data,features_data,by='feature')
##rename column 'pricing_plan_id' to 'pricing_plan_id_original' to avoid confusion
names(plans_and_features)<-c('feature','pricing_plan_id_original','app_id','pricing_feature_id')
##mapping new pricing_plan_id: step 1: by 'pricing_plan_id_original'
plans_and_features<-merge(plans_and_features,pricing_plans_data,by='pricing_plan_id_original')
##mapping new pricing_plan_id: step 2: add 'pricing_plan_id'
plans_and_features<-merge(plans_and_features,pricing_plans_unique,by=c('price_plan','title'))
##select useful columns and unique the combination
pricing_plans_and_pricing_features_data = unique(plans_and_features[c('pricing_plan_id', 'pricing_feature_id')])
##write table
dbWriteTable(con,name='pricing_plans_and_pricing_features',value=pricing_plans_and_pricing_features_data,row.names=FALSE,append=TRUE)

#table15:reviews
library(lubridate)
reviews_data = read.csv('C:/Users/luoyu/Desktop/SQL/project/shopify data/reviews.csv', stringsAsFactors = F)
colnames(reviews_data) = c("app_id", "author", "body", "rating", "helpful_counts", "posted_at", "developer_reply", "developer_reply_posted_at")
reviews_data$posted_at = mdy(reviews_data$posted_at)
reviews_data$developer_reply_posted_at = mdy(reviews_data$developer_reply_posted_at)
reviews = reviews_data[c("author", "body", "rating", "posted_at", "helpful_counts")]
reviews_unique = unique(reviews)
reviews_unique$review_id = 1:nrow(reviews_unique)
##write table
dbWriteTable(con,name='reviews',value=reviews_unique,row.names=FALSE,append=TRUE)

#table16:apps_and_reviews
reviews_data = merge(reviews_data,reviews_unique,by=c("author", "body", "rating", "posted_at", "helpful_counts"))
apps_and_reviews = reviews_data[c("app_id", "review_id")]
apps_and_reviews = unique(apps_and_reviews)
##write table
dbWriteTable(con,name='apps_and_reviews',value=apps_and_reviews,row.names=FALSE,append=TRUE)

#table17:developer_replies
developer_replies_data = reviews_data[c("developer_reply", "developer_reply_posted_at")]
developer_replies_unique = unique(developer_replies_data)
developer_replies_unique$developer_reply_id = 1:nrow(developer_replies_unique)
##write table
dbWriteTable(con,name='developer_replies',value=developer_replies_unique,row.names=FALSE,append=TRUE)

#table18:reviews_and_developer_replies
developer_replies_data = merge(reviews_data,developer_replies_unique,by=c("developer_reply", "developer_reply_posted_at"))
reviews_and_developer_replies = developer_replies_data[c("review_id", "developer_reply_id")]
reviews_and_developer_replies = unique(reviews_and_developer_replies)
##write table
dbWriteTable(con,name='reviews_and_developer_replies',value=reviews_and_developer_replies,row.names=FALSE,append=TRUE)

#disconnection
dbDisconnect(con)
dbUnloadDriver(drv)
