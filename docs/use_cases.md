# Use Cases
We list some of the use cases that Splash-ML was created to support

## ETL
### Ingest Historic Data from a filesystem
User has data stored in the file system. They run the ETLExecutor, pointing a path on the files sytem. The executor reads the file system and all subfolder and creates a Blue Sky document model set. It transforms the source files into a jpeg of a given size and adds it to he "thumbnail" document stream.

## Tag Service

### What models do I have?
User  asks the TagService for all models known to the system. Information is returned about each model known to the system and when the model was created.

### What tags do I have?
User asks the TagService for all tags known to the system. Once returned, the user may ask for a list of all assets with a given tag.

### What images do I have with no tags?
User asks the TagService for a list of all assets without a tag.

### Give me all the images with tag="foo"
User asks the TagService for a list of all assets that have a tag of foo. Optionally, the user may provide a range of confidence values in the query.

### Collect training set
User wants to create a training and testing set that have already been tagged. The user asks the TagService for  a list of images with multiple tags. A list is provided, with the tags for each image. The system can then create a folder structure to feed an ML model (organized by tag).
