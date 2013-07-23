// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: popeye/transport/proto/storage.proto

package popeye.transport.proto;

public final class Storage {
  private Storage() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface EnsembleOrBuilder
      extends com.google.protobuf.MessageOrBuilder {
    
    // required int64 batchId = 1;
    boolean hasBatchId();
    long getBatchId();
    
    // required int32 partition = 2;
    boolean hasPartition();
    int getPartition();
    
    // repeated .popeye.transport.proto.Event events = 3;
    java.util.List<popeye.transport.proto.Message.Event> 
        getEventsList();
    popeye.transport.proto.Message.Event getEvents(int index);
    int getEventsCount();
    java.util.List<? extends popeye.transport.proto.Message.EventOrBuilder> 
        getEventsOrBuilderList();
    popeye.transport.proto.Message.EventOrBuilder getEventsOrBuilder(
        int index);
  }
  public static final class Ensemble extends
      com.google.protobuf.GeneratedMessage
      implements EnsembleOrBuilder {
    // Use Ensemble.newBuilder() to construct.
    private Ensemble(Builder builder) {
      super(builder);
    }
    private Ensemble(boolean noInit) {}
    
    private static final Ensemble defaultInstance;
    public static Ensemble getDefaultInstance() {
      return defaultInstance;
    }
    
    public Ensemble getDefaultInstanceForType() {
      return defaultInstance;
    }
    
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return popeye.transport.proto.Storage.internal_static_popeye_transport_proto_Ensemble_descriptor;
    }
    
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return popeye.transport.proto.Storage.internal_static_popeye_transport_proto_Ensemble_fieldAccessorTable;
    }
    
    private int bitField0_;
    // required int64 batchId = 1;
    public static final int BATCHID_FIELD_NUMBER = 1;
    private long batchId_;
    public boolean hasBatchId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    public long getBatchId() {
      return batchId_;
    }
    
    // required int32 partition = 2;
    public static final int PARTITION_FIELD_NUMBER = 2;
    private int partition_;
    public boolean hasPartition() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    public int getPartition() {
      return partition_;
    }
    
    // repeated .popeye.transport.proto.Event events = 3;
    public static final int EVENTS_FIELD_NUMBER = 3;
    private java.util.List<popeye.transport.proto.Message.Event> events_;
    public java.util.List<popeye.transport.proto.Message.Event> getEventsList() {
      return events_;
    }
    public java.util.List<? extends popeye.transport.proto.Message.EventOrBuilder> 
        getEventsOrBuilderList() {
      return events_;
    }
    public int getEventsCount() {
      return events_.size();
    }
    public popeye.transport.proto.Message.Event getEvents(int index) {
      return events_.get(index);
    }
    public popeye.transport.proto.Message.EventOrBuilder getEventsOrBuilder(
        int index) {
      return events_.get(index);
    }
    
    private void initFields() {
      batchId_ = 0L;
      partition_ = 0;
      events_ = java.util.Collections.emptyList();
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;
      
      if (!hasBatchId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasPartition()) {
        memoizedIsInitialized = 0;
        return false;
      }
      for (int i = 0; i < getEventsCount(); i++) {
        if (!getEvents(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      memoizedIsInitialized = 1;
      return true;
    }
    
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeInt64(1, batchId_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeInt32(2, partition_);
      }
      for (int i = 0; i < events_.size(); i++) {
        output.writeMessage(3, events_.get(i));
      }
      getUnknownFields().writeTo(output);
    }
    
    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;
    
      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(1, batchId_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(2, partition_);
      }
      for (int i = 0; i < events_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(3, events_.get(i));
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }
    
    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }
    
    public static popeye.transport.proto.Storage.Ensemble parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static popeye.transport.proto.Storage.Ensemble parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static popeye.transport.proto.Storage.Ensemble parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static popeye.transport.proto.Storage.Ensemble parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static popeye.transport.proto.Storage.Ensemble parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static popeye.transport.proto.Storage.Ensemble parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    public static popeye.transport.proto.Storage.Ensemble parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static popeye.transport.proto.Storage.Ensemble parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input, extensionRegistry)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static popeye.transport.proto.Storage.Ensemble parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static popeye.transport.proto.Storage.Ensemble parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    
    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(popeye.transport.proto.Storage.Ensemble prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }
    
    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements popeye.transport.proto.Storage.EnsembleOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return popeye.transport.proto.Storage.internal_static_popeye_transport_proto_Ensemble_descriptor;
      }
      
      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return popeye.transport.proto.Storage.internal_static_popeye_transport_proto_Ensemble_fieldAccessorTable;
      }
      
      // Construct using popeye.transport.proto.Storage.Ensemble.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }
      
      private Builder(BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
          getEventsFieldBuilder();
        }
      }
      private static Builder create() {
        return new Builder();
      }
      
      public Builder clear() {
        super.clear();
        batchId_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000001);
        partition_ = 0;
        bitField0_ = (bitField0_ & ~0x00000002);
        if (eventsBuilder_ == null) {
          events_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000004);
        } else {
          eventsBuilder_.clear();
        }
        return this;
      }
      
      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }
      
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return popeye.transport.proto.Storage.Ensemble.getDescriptor();
      }
      
      public popeye.transport.proto.Storage.Ensemble getDefaultInstanceForType() {
        return popeye.transport.proto.Storage.Ensemble.getDefaultInstance();
      }
      
      public popeye.transport.proto.Storage.Ensemble build() {
        popeye.transport.proto.Storage.Ensemble result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }
      
      private popeye.transport.proto.Storage.Ensemble buildParsed()
          throws com.google.protobuf.InvalidProtocolBufferException {
        popeye.transport.proto.Storage.Ensemble result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(
            result).asInvalidProtocolBufferException();
        }
        return result;
      }
      
      public popeye.transport.proto.Storage.Ensemble buildPartial() {
        popeye.transport.proto.Storage.Ensemble result = new popeye.transport.proto.Storage.Ensemble(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.batchId_ = batchId_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.partition_ = partition_;
        if (eventsBuilder_ == null) {
          if (((bitField0_ & 0x00000004) == 0x00000004)) {
            events_ = java.util.Collections.unmodifiableList(events_);
            bitField0_ = (bitField0_ & ~0x00000004);
          }
          result.events_ = events_;
        } else {
          result.events_ = eventsBuilder_.build();
        }
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }
      
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof popeye.transport.proto.Storage.Ensemble) {
          return mergeFrom((popeye.transport.proto.Storage.Ensemble)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }
      
      public Builder mergeFrom(popeye.transport.proto.Storage.Ensemble other) {
        if (other == popeye.transport.proto.Storage.Ensemble.getDefaultInstance()) return this;
        if (other.hasBatchId()) {
          setBatchId(other.getBatchId());
        }
        if (other.hasPartition()) {
          setPartition(other.getPartition());
        }
        if (eventsBuilder_ == null) {
          if (!other.events_.isEmpty()) {
            if (events_.isEmpty()) {
              events_ = other.events_;
              bitField0_ = (bitField0_ & ~0x00000004);
            } else {
              ensureEventsIsMutable();
              events_.addAll(other.events_);
            }
            onChanged();
          }
        } else {
          if (!other.events_.isEmpty()) {
            if (eventsBuilder_.isEmpty()) {
              eventsBuilder_.dispose();
              eventsBuilder_ = null;
              events_ = other.events_;
              bitField0_ = (bitField0_ & ~0x00000004);
              eventsBuilder_ = 
                com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders ?
                   getEventsFieldBuilder() : null;
            } else {
              eventsBuilder_.addAllMessages(other.events_);
            }
          }
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }
      
      public final boolean isInitialized() {
        if (!hasBatchId()) {
          
          return false;
        }
        if (!hasPartition()) {
          
          return false;
        }
        for (int i = 0; i < getEventsCount(); i++) {
          if (!getEvents(i).isInitialized()) {
            
            return false;
          }
        }
        return true;
      }
      
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder(
            this.getUnknownFields());
        while (true) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              this.setUnknownFields(unknownFields.build());
              onChanged();
              return this;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                this.setUnknownFields(unknownFields.build());
                onChanged();
                return this;
              }
              break;
            }
            case 8: {
              bitField0_ |= 0x00000001;
              batchId_ = input.readInt64();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              partition_ = input.readInt32();
              break;
            }
            case 26: {
              popeye.transport.proto.Message.Event.Builder subBuilder = popeye.transport.proto.Message.Event.newBuilder();
              input.readMessage(subBuilder, extensionRegistry);
              addEvents(subBuilder.buildPartial());
              break;
            }
          }
        }
      }
      
      private int bitField0_;
      
      // required int64 batchId = 1;
      private long batchId_ ;
      public boolean hasBatchId() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      public long getBatchId() {
        return batchId_;
      }
      public Builder setBatchId(long value) {
        bitField0_ |= 0x00000001;
        batchId_ = value;
        onChanged();
        return this;
      }
      public Builder clearBatchId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        batchId_ = 0L;
        onChanged();
        return this;
      }
      
      // required int32 partition = 2;
      private int partition_ ;
      public boolean hasPartition() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      public int getPartition() {
        return partition_;
      }
      public Builder setPartition(int value) {
        bitField0_ |= 0x00000002;
        partition_ = value;
        onChanged();
        return this;
      }
      public Builder clearPartition() {
        bitField0_ = (bitField0_ & ~0x00000002);
        partition_ = 0;
        onChanged();
        return this;
      }
      
      // repeated .popeye.transport.proto.Event events = 3;
      private java.util.List<popeye.transport.proto.Message.Event> events_ =
        java.util.Collections.emptyList();
      private void ensureEventsIsMutable() {
        if (!((bitField0_ & 0x00000004) == 0x00000004)) {
          events_ = new java.util.ArrayList<popeye.transport.proto.Message.Event>(events_);
          bitField0_ |= 0x00000004;
         }
      }
      
      private com.google.protobuf.RepeatedFieldBuilder<
          popeye.transport.proto.Message.Event, popeye.transport.proto.Message.Event.Builder, popeye.transport.proto.Message.EventOrBuilder> eventsBuilder_;
      
      public java.util.List<popeye.transport.proto.Message.Event> getEventsList() {
        if (eventsBuilder_ == null) {
          return java.util.Collections.unmodifiableList(events_);
        } else {
          return eventsBuilder_.getMessageList();
        }
      }
      public int getEventsCount() {
        if (eventsBuilder_ == null) {
          return events_.size();
        } else {
          return eventsBuilder_.getCount();
        }
      }
      public popeye.transport.proto.Message.Event getEvents(int index) {
        if (eventsBuilder_ == null) {
          return events_.get(index);
        } else {
          return eventsBuilder_.getMessage(index);
        }
      }
      public Builder setEvents(
          int index, popeye.transport.proto.Message.Event value) {
        if (eventsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureEventsIsMutable();
          events_.set(index, value);
          onChanged();
        } else {
          eventsBuilder_.setMessage(index, value);
        }
        return this;
      }
      public Builder setEvents(
          int index, popeye.transport.proto.Message.Event.Builder builderForValue) {
        if (eventsBuilder_ == null) {
          ensureEventsIsMutable();
          events_.set(index, builderForValue.build());
          onChanged();
        } else {
          eventsBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      public Builder addEvents(popeye.transport.proto.Message.Event value) {
        if (eventsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureEventsIsMutable();
          events_.add(value);
          onChanged();
        } else {
          eventsBuilder_.addMessage(value);
        }
        return this;
      }
      public Builder addEvents(
          int index, popeye.transport.proto.Message.Event value) {
        if (eventsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureEventsIsMutable();
          events_.add(index, value);
          onChanged();
        } else {
          eventsBuilder_.addMessage(index, value);
        }
        return this;
      }
      public Builder addEvents(
          popeye.transport.proto.Message.Event.Builder builderForValue) {
        if (eventsBuilder_ == null) {
          ensureEventsIsMutable();
          events_.add(builderForValue.build());
          onChanged();
        } else {
          eventsBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      public Builder addEvents(
          int index, popeye.transport.proto.Message.Event.Builder builderForValue) {
        if (eventsBuilder_ == null) {
          ensureEventsIsMutable();
          events_.add(index, builderForValue.build());
          onChanged();
        } else {
          eventsBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      public Builder addAllEvents(
          java.lang.Iterable<? extends popeye.transport.proto.Message.Event> values) {
        if (eventsBuilder_ == null) {
          ensureEventsIsMutable();
          super.addAll(values, events_);
          onChanged();
        } else {
          eventsBuilder_.addAllMessages(values);
        }
        return this;
      }
      public Builder clearEvents() {
        if (eventsBuilder_ == null) {
          events_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000004);
          onChanged();
        } else {
          eventsBuilder_.clear();
        }
        return this;
      }
      public Builder removeEvents(int index) {
        if (eventsBuilder_ == null) {
          ensureEventsIsMutable();
          events_.remove(index);
          onChanged();
        } else {
          eventsBuilder_.remove(index);
        }
        return this;
      }
      public popeye.transport.proto.Message.Event.Builder getEventsBuilder(
          int index) {
        return getEventsFieldBuilder().getBuilder(index);
      }
      public popeye.transport.proto.Message.EventOrBuilder getEventsOrBuilder(
          int index) {
        if (eventsBuilder_ == null) {
          return events_.get(index);  } else {
          return eventsBuilder_.getMessageOrBuilder(index);
        }
      }
      public java.util.List<? extends popeye.transport.proto.Message.EventOrBuilder> 
           getEventsOrBuilderList() {
        if (eventsBuilder_ != null) {
          return eventsBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(events_);
        }
      }
      public popeye.transport.proto.Message.Event.Builder addEventsBuilder() {
        return getEventsFieldBuilder().addBuilder(
            popeye.transport.proto.Message.Event.getDefaultInstance());
      }
      public popeye.transport.proto.Message.Event.Builder addEventsBuilder(
          int index) {
        return getEventsFieldBuilder().addBuilder(
            index, popeye.transport.proto.Message.Event.getDefaultInstance());
      }
      public java.util.List<popeye.transport.proto.Message.Event.Builder> 
           getEventsBuilderList() {
        return getEventsFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilder<
          popeye.transport.proto.Message.Event, popeye.transport.proto.Message.Event.Builder, popeye.transport.proto.Message.EventOrBuilder> 
          getEventsFieldBuilder() {
        if (eventsBuilder_ == null) {
          eventsBuilder_ = new com.google.protobuf.RepeatedFieldBuilder<
              popeye.transport.proto.Message.Event, popeye.transport.proto.Message.Event.Builder, popeye.transport.proto.Message.EventOrBuilder>(
                  events_,
                  ((bitField0_ & 0x00000004) == 0x00000004),
                  getParentForChildren(),
                  isClean());
          events_ = null;
        }
        return eventsBuilder_;
      }
      
      // @@protoc_insertion_point(builder_scope:popeye.transport.proto.Ensemble)
    }
    
    static {
      defaultInstance = new Ensemble(true);
      defaultInstance.initFields();
    }
    
    // @@protoc_insertion_point(class_scope:popeye.transport.proto.Ensemble)
  }
  
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_popeye_transport_proto_Ensemble_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_popeye_transport_proto_Ensemble_fieldAccessorTable;
  
  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n$popeye/transport/proto/storage.proto\022\026" +
      "popeye.transport.proto\032$popeye/transport" +
      "/proto/message.proto\"]\n\010Ensemble\022\017\n\007batc" +
      "hId\030\001 \002(\003\022\021\n\tpartition\030\002 \002(\005\022-\n\006events\030\003" +
      " \003(\0132\035.popeye.transport.proto.Event"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_popeye_transport_proto_Ensemble_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_popeye_transport_proto_Ensemble_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_popeye_transport_proto_Ensemble_descriptor,
              new java.lang.String[] { "BatchId", "Partition", "Events", },
              popeye.transport.proto.Storage.Ensemble.class,
              popeye.transport.proto.Storage.Ensemble.Builder.class);
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          popeye.transport.proto.Message.getDescriptor(),
        }, assigner);
  }
  
  // @@protoc_insertion_point(outer_class_scope)
}
