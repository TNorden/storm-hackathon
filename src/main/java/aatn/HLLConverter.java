package aatn;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.http.util.Constants;
import com.twitter.algebird.HLL;
import com.twitter.algebird.HyperLogLog;
import com.twitter.algebird.HyperLogLogMonoid;

import static com.basho.riak.client.convert.KeyUtil.getKey;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 7/20/13
 * Time: 4:59 PM
 */
public class HLLConverter implements Converter<HLL> {

  String bucket;

  public HLLConverter(String bucket) {
    this.bucket = bucket;
  }

  @Override
  public IRiakObject fromDomain(HLL domainObject, VClock vclock) throws ConversionException {

    String key = getKey(domainObject);

    byte[] value = HyperLogLog.toBytes(domainObject);

    return RiakObjectBuilder.newBuilder(this.bucket, key)
        .withValue(value)
        .withVClock(vclock)
        .withContentType(Constants.CTYPE_OCTET_STREAM)
        .build();
  }

  @Override
  public HLL toDomain(IRiakObject riakObject) throws ConversionException {
    if (riakObject == null) {
      HyperLogLogMonoid hllm = new HyperLogLogMonoid(12);
      return hllm.zero();
    }
    HyperLogLog.fromBytes(riakObject.getValue());
    return HyperLogLog.fromBytes(riakObject.getValue());
  }
}
