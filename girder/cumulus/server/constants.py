

class ClusterType:
    EC2 = 'ec2'
    TRADITIONAL = 'trad'

    @staticmethod
    def is_valid_type(type):
        return type == ClusterType.EC2 or type == ClusterType.TRADITIONAL
